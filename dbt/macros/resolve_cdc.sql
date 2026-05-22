{% macro resolve_cdc(source_ref, model_name=none) %}
{#
    Resolves a Bronze CDC table into current entity state for Silver.
    Reads strategy from TABLE_METADATA via get_table_config().

    Strategies:
        upsert  → deduplicate by unique_key + filter is_deleted
                  (entity tables: usuarios, produtos, clientes)
        append  → no deduplication, all rows kept
                  (fact tables: vendas, itens_venda)
        log     → append-only, never filter
                  (log tables: estoque_movimentos, auditoria)

    Args:
        source_ref  : ref() pointing to the Bronze model
        model_name  : name of the calling model (default: this.name)

    Usage in Silver model:
        {{ resolve_cdc(ref('bronze_usuarios')) }}
        {{ resolve_cdc(ref('bronze_vendas'), model_name='silver_vendas') }}
#}

{% set calling_model = model_name or this.name %}
{% set cfg = get_config_for(calling_model) %}
{% set strategy  = cfg.get('cdc_strategy', 'upsert') %}
{% set id_col    = cfg.get('unique_key', 'id') %}

{% if strategy == 'upsert' %}

    {# Entity table: one row per id, latest non-deleted state #}
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ id_col }}
                ORDER BY source_ts_ms DESC
            ) AS _cdc_row_num
        FROM {{ source_ref }}
    )
    SELECT * EXCLUDE (_cdc_row_num)
    FROM ranked
    WHERE _cdc_row_num = 1
      AND op != 'd'

{% elif strategy == 'append' %}

    {# Fact table: all rows, no deduplication, filter only hard deletes #}
    SELECT *
    FROM {{ source_ref }}
    WHERE op != 'd'

{% elif strategy == 'log' %}

    {# Log/audit table: every row kept, including deletes as records #}
    SELECT *
    FROM {{ source_ref }}

{% else %}

    {{ exceptions.raise_compiler_error(
        "Unknown cdc_strategy '" ~ strategy ~ "' for model " ~ calling_model ~
        ". Valid values: upsert | append | log. " ~
        "Check CONFIG.TABLE_METADATA or run sync_metadata.py."
    ) }}

{% endif %}

{% endmacro %}
