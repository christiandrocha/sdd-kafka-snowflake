{% macro get_table_config() %}
{#
    Reads CONFIG.TABLE_METADATA once per dbt run and returns a dict
    keyed by table_name. Cached in a Jinja global so subsequent calls
    within the same run do not hit Snowflake again.

    Returns dict shape:
    {
        'usuarios': {
            'table_type':   'entity',
            'cdc_strategy': 'upsert',
            'unique_key':   'id',
            'active':       true
        },
        ...
    }

    Usage in a model:
        {% set config = get_table_config() %}
        {% set table_cfg = config.get(this.name | replace('bronze_','') | replace('silver_',''), {}) %}
        {% set strategy = table_cfg.get('cdc_strategy', 'upsert') %}
        {% set unique_key = table_cfg.get('unique_key', 'id') %}
#}

{# Return cached config if already loaded this run #}
{% if '_table_config_cache' in context %}
    {{ return(context._table_config_cache) }}
{% endif %}

{#
    Static fallback used at parse time (execute=False) when no DB connection
    is available. Keeps unique_key in config() correct for dbt MERGE strategy.
    Source of truth remains TABLE_METADATA at execution time.
#}
{% set static_fallback = {
    'payment_events':  {'table_type': 'fact',   'cdc_strategy': 'upsert', 'unique_key': 'event_id',        'active': true},
    'gps_events':      {'table_type': 'log',    'cdc_strategy': 'upsert', 'unique_key': 'gps_id',          'active': true},
    'order_status':    {'table_type': 'log',    'cdc_strategy': 'upsert', 'unique_key': 'status_id',       'active': true},
    'search_events':   {'table_type': 'log',    'cdc_strategy': 'upsert', 'unique_key': 'search_id',       'active': true},
    'recommendations': {'table_type': 'log',    'cdc_strategy': 'upsert', 'unique_key': 'event_id',        'active': true},
    'orders':          {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'order_id',        'active': true},
    'payments':        {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'payment_id',      'active': true},
    'routes':          {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'route_id',        'active': true},
    'receipts':        {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'receipt_id',      'active': true},
    'driver_shifts':   {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'shift_id',        'active': true},
    'support_tickets': {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'ticket_id',       'active': true},
    'users_mongo':     {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',            'active': true},
    'users_mssql':     {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',            'active': true},
    'restaurants':     {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',            'active': true},
    'drivers':         {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',            'active': true},
    'products':        {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'product_id',      'active': true},
    'menu_sections':   {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'menu_section_id', 'active': true},
    'ratings':         {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'rating_id',       'active': true},
    'inventory':       {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'stock_id',        'active': true},
    'order_items':     {'table_type': 'fact',   'cdc_strategy': 'upsert', 'unique_key': 'order_item_id',   'active': true}
} %}

{% if not execute %}
    {{ return(static_fallback) }}
{% endif %}

{# Load from Snowflake TABLE_METADATA at execution time #}
{% set query %}
    SELECT
        table_name,
        table_type,
        cdc_strategy,
        unique_key,
        active
    FROM {{ target.database }}.CONFIG.TABLE_METADATA
    WHERE active = true
{% endset %}

{% set config_dict = {} %}
{% set results = run_query(query) %}
{% for row in results %}
    {% set _ = config_dict.update({
        row[0]: {
            'table_type':   row[1],
            'cdc_strategy': row[2],
            'unique_key':   row[3],
            'active':       row[4]
        }
    }) %}
{% endfor %}

{# Fall back to static map for any table not yet in TABLE_METADATA #}
{% for k, v in static_fallback.items() %}
    {% if k not in config_dict %}
        {% set _ = config_dict.update({k: v}) %}
    {% endif %}
{% endfor %}

{# Cache for this run #}
{% set _ = context.update({'_table_config_cache': config_dict}) %}

{{ return(config_dict) }}

{% endmacro %}


{% macro get_config_for(model_name) %}
{#
    Convenience wrapper — returns config for a single table.
    Strips bronze_/silver_/gold_ prefix to find the table name.

    Usage:
        {% set cfg = get_config_for('bronze_usuarios') %}
        {% set strategy = cfg.get('cdc_strategy', 'upsert') %}
#}

{% set all_config = get_table_config() %}
{% set clean_name = model_name
    | replace('bronze_', '')
    | replace('silver_', '')
    | replace('gold_', '') %}

{% set default_config = {
    'table_type':   'entity',
    'cdc_strategy': 'upsert',
    'unique_key':   'id',
    'active':       true
} %}

{% set result = all_config.get(clean_name, default_config) %}

{# Fail loud if table is not in metadata and not using default #}
{% if clean_name not in all_config %}
    {{ log(
        "WARNING: " ~ clean_name ~ " not found in TABLE_METADATA. "
        "Using defaults: " ~ default_config | tojson ~ ". "
        "Run sync_metadata.py to register this table.",
        info=true
    ) }}
{% endif %}

{{ return(result) }}

{% endmacro %}
