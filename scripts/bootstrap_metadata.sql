-- ──────────────────────────────────────────────────────────────────────────
-- bootstrap_metadata.sql
-- Creates CONFIG schema and populates TABLE_METADATA for all 20 domains.
-- Run once before starting the pipeline.
-- ──────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS CDC_POC.CONFIG;

GRANT USAGE  ON SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;

-- ── TABLE_METADATA ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CDC_POC.CONFIG.TABLE_METADATA (
    table_name          VARCHAR(100)  NOT NULL,
    topic               VARCHAR(200)  NOT NULL,
    table_type          VARCHAR(20)   NOT NULL,
    cdc_strategy        VARCHAR(20)   NOT NULL,
    unique_key          VARCHAR(100),
    active              BOOLEAN       NOT NULL DEFAULT true,
    registered_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source              VARCHAR(50)   NOT NULL,
    previous_strategy   VARCHAR(20),
    changed_by          VARCHAR(100),
    notes               VARCHAR(500),
    CONSTRAINT pk_table_metadata PRIMARY KEY (table_name)
);

-- ── METADATA_HISTORY ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CDC_POC.CONFIG.METADATA_HISTORY (
    history_id    NUMBER        AUTOINCREMENT PRIMARY KEY,
    table_name    VARCHAR(100)  NOT NULL,
    changed_at    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    changed_by    VARCHAR(100)  NOT NULL,
    change_type   VARCHAR(20)   NOT NULL,
    field_changed VARCHAR(100),
    old_value     VARCHAR(500),
    new_value     VARCHAR(500),
    source        VARCHAR(50)   NOT NULL
);

-- ── PROCESSING_LOG ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CDC_POC.CONFIG.PROCESSING_LOG (
    log_id            NUMBER        AUTOINCREMENT PRIMARY KEY,
    table_name        VARCHAR(100)  NOT NULL,
    layer             VARCHAR(20)   NOT NULL,
    dbt_model         VARCHAR(200)  NOT NULL,
    dbt_invocation_id VARCHAR(100),
    run_id            VARCHAR(100),
    status            VARCHAR(20)   NOT NULL,
    rows_processed    NUMBER        DEFAULT 0,
    rows_inserted     NUMBER        DEFAULT 0,
    rows_updated      NUMBER        DEFAULT 0,
    rows_deleted      NUMBER        DEFAULT 0,
    started_at        TIMESTAMP_NTZ,
    finished_at       TIMESTAMP_NTZ,
    duration_seconds  NUMBER(10,3),
    error_message     VARCHAR(2000),
    triggered_by      VARCHAR(100),
    logged_at         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ── TABLE_METADATA initial data — all 20 domains ─────────────────────────────
INSERT INTO CDC_POC.CONFIG.TABLE_METADATA
    (table_name, topic, table_type, cdc_strategy, unique_key, source, changed_by, notes)
VALUES
    -- Event sourcing tables (append-only by nature, upsert by PK for idempotency)
    ('payment_events',  'pg.public.payment_events',  'fact',   'upsert', 'event_id',       'manual', 'bootstrap', 'Payment lifecycle events. Nested event JSONB. Timestamp int/float.'),
    ('gps_events',      'pg.public.gps_events',      'log',    'upsert', 'gps_id',         'manual', 'bootstrap', 'High-volume GPS tracking. 7,350 records across 5 files.'),
    ('order_status',    'pg.public.order_status',    'log',    'upsert', 'status_id',      'manual', 'bootstrap', 'Order status events. Nested status JSONB.'),
    ('search_events',   'pg.public.search_events',   'log',    'upsert', 'search_id',      'manual', 'bootstrap', 'User search queries.'),
    ('recommendations', 'pg.public.recommendations', 'log',    'upsert', 'event_id',       'manual', 'bootstrap', 'ML recommendation events.'),

    -- Entity tables (upsert by PK — state snapshot)
    ('orders',          'pg.public.orders',          'entity', 'upsert', 'order_id',       'manual', 'bootstrap', 'Hub table. Links all domains via *_key fields (CPF, CNPJ, driver_id).'),
    ('payments',        'pg.public.payments',        'entity', 'upsert', 'payment_id',     'manual', 'bootstrap', 'Full payment details: card, method, amounts, provider.'),
    ('routes',          'pg.public.routes',          'entity', 'upsert', 'route_id',       'manual', 'bootstrap', 'Delivery routes per order.'),
    ('receipts',        'pg.public.receipts',        'entity', 'upsert', 'receipt_id',     'manual', 'bootstrap', 'Financial receipts. Uses receipt_generated_at (no dt_current_timestamp).'),
    ('driver_shifts',   'pg.public.driver_shifts',   'entity', 'upsert', 'shift_id',       'manual', 'bootstrap', 'Driver shift performance: earnings, orders, distance.'),
    ('support_tickets', 'pg.public.support_tickets', 'entity', 'upsert', 'ticket_id',      'manual', 'bootstrap', 'Customer support tickets.'),
    ('users_mongo',     'pg.public.users_mongo',     'entity', 'upsert', 'uuid',           'manual', 'bootstrap', 'Users from MongoDB. CPF = user_key in orders.'),
    ('users_mssql',     'pg.public.users_mssql',     'entity', 'upsert', 'uuid',           'manual', 'bootstrap', 'Extended user profiles from MSSQL. Same CPF key.'),
    ('restaurants',     'pg.public.restaurants',     'entity', 'upsert', 'uuid',           'manual', 'bootstrap', 'Restaurants from MySQL. CNPJ = restaurant_key in orders.'),
    ('drivers',         'pg.public.drivers',         'entity', 'upsert', 'uuid',           'manual', 'bootstrap', 'Drivers from PostgreSQL. driver_id = driver_key in orders.'),
    ('products',        'pg.public.products',        'entity', 'upsert', 'product_id',     'manual', 'bootstrap', 'Products from MySQL. product_id (PRD-XXXXX) = FK in order_items.'),
    ('menu_sections',   'pg.public.menu_sections',   'entity', 'upsert', 'menu_section_id','manual', 'bootstrap', 'Restaurant menu sections from MySQL.'),
    ('ratings',         'pg.public.ratings',         'entity', 'upsert', 'rating_id',      'manual', 'bootstrap', 'Restaurant ratings. rating_key in orders = uuid here.'),
    ('inventory',       'pg.public.inventory',       'entity', 'upsert', 'stock_id',       'manual', 'bootstrap', 'Stock levels per product per restaurant. Uses last_updated (no dt_current_timestamp).'),

    -- Fact table (high volume)
    ('order_items',     'pg.public.order_items',     'fact',   'upsert', 'order_item_id',  'manual', 'bootstrap', 'Order line items from MongoDB. 110,001 records — largest table.')

ON CONFLICT (table_name) DO NOTHING;

-- ── Verify ────────────────────────────────────────────────────────────────────
SELECT table_name, table_type, cdc_strategy, unique_key
FROM CDC_POC.CONFIG.TABLE_METADATA
ORDER BY table_type, table_name;
