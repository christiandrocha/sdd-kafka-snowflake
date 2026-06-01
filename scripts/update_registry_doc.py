#!/usr/bin/env python3
"""
update_registry_doc.py
Updates the Avro 'doc' field in Schema Registry subjects with CDC metadata.

The doc field drives sync_metadata.py — without it, all tables default to
table_type=entity and unique_key=id. This script patches each subject with
the correct values from the domain map.

After running this script, execute sync_metadata.py to propagate changes
to CONFIG.TABLE_METADATA.

Usage:
    python3 scripts/update_registry_doc.py [--dry-run]
"""

import argparse
import json
import logging
import os
import sys

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Domain map: table_name → (table_type, cdc_strategy, unique_key)
# Source: CLAUDE.md domain map + ADR-06 (all tables upsert for Snowpipe idempotency)
DOMAIN_METADATA = {
    "payment_events":  ("event",  "upsert", "event_id"),
    "gps_events":      ("event",  "upsert", "gps_id"),
    "order_status":    ("event",  "upsert", "status_id"),
    "search_events":   ("event",  "upsert", "search_id"),
    "recommendations": ("event",  "upsert", "event_id"),
    "order_items":     ("fact",   "upsert", "order_item_id"),
    "orders":          ("entity", "upsert", "order_id"),
    "payments":        ("entity", "upsert", "payment_id"),
    "routes":          ("entity", "upsert", "route_id"),
    "receipts":        ("entity", "upsert", "receipt_id"),
    "driver_shifts":   ("entity", "upsert", "shift_id"),
    "support_tickets": ("entity", "upsert", "ticket_id"),
    "users_mongo":     ("entity", "upsert", "uuid"),
    "users_mssql":     ("entity", "upsert", "uuid"),
    "restaurants":     ("entity", "upsert", "uuid"),
    "drivers":         ("entity", "upsert", "uuid"),
    "products":        ("entity", "upsert", "product_id"),
    "menu_sections":   ("entity", "upsert", "menu_section_id"),
    "ratings":         ("entity", "upsert", "rating_id"),
    "inventory":       ("entity", "upsert", "stock_id"),
}


def get_subjects() -> list[str]:
    resp = requests.get(f"{REGISTRY_URL}/subjects", timeout=10)
    resp.raise_for_status()
    return [s for s in resp.json() if s.endswith("-value")]


def get_latest_schema(subject: str) -> tuple[int, dict]:
    """Returns (schema_id, avro_schema_dict)."""
    resp = requests.get(
        f"{REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10
    )
    resp.raise_for_status()
    data = resp.json()
    return data["id"], json.loads(data["schema"])


def register_schema(subject: str, schema: dict) -> int:
    """Registers a new version of the schema. Returns new schema_id."""
    resp = requests.post(
        f"{REGISTRY_URL}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json={"schema": json.dumps(schema)},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def subject_to_table(subject: str) -> str:
    return subject.removesuffix("-value").split(".")[-1]


def update_schemas(dry_run: bool) -> dict:
    subjects = get_subjects()
    log.info(f"Found {len(subjects)} subjects in Schema Registry")

    summary = {"updated": 0, "skipped": 0, "unknown": 0, "errors": 0}

    for subject in subjects:
        table = subject_to_table(subject)

        if table not in DOMAIN_METADATA:
            log.warning(f"  [UNKNOWN] {subject} — no metadata entry for '{table}'")
            summary["unknown"] += 1
            continue

        table_type, cdc_strategy, unique_key = DOMAIN_METADATA[table]
        doc_value = f"table_type={table_type},cdc_strategy={cdc_strategy},unique_key={unique_key}"

        try:
            schema_id, avro_schema = get_latest_schema(subject)

            if avro_schema.get("doc") == doc_value:
                log.info(f"  [SKIP] {subject} — doc already correct")
                summary["skipped"] += 1
                continue

            old_doc = avro_schema.get("doc")
            avro_schema["doc"] = doc_value

            log.info(f"  [UPDATE] {subject}")
            log.info(f"    old doc: {old_doc!r}")
            log.info(f"    new doc: {doc_value!r}")

            if not dry_run:
                new_id = register_schema(subject, avro_schema)
                log.info(f"    schema_id: {schema_id} → {new_id}")

            summary["updated"] += 1

        except Exception as exc:
            log.error(f"  [ERROR] {subject}: {exc}")
            summary["errors"] += 1

    log.info(
        f"Done — updated: {summary['updated']}, skipped: {summary['skipped']}, "
        f"unknown: {summary['unknown']}, errors: {summary['errors']}"
    )
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patch Avro doc field in Schema Registry")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing to Registry")
    args = parser.parse_args()

    if args.dry_run:
        log.info("DRY RUN — no changes will be written")

    summary = update_schemas(dry_run=args.dry_run)
    sys.exit(1 if summary["errors"] > 0 else 0)
