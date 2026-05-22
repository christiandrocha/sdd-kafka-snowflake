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
{% if modules.builtins.hasattr(context, '_table_config_cache') %}
    {{ return(context._table_config_cache) }}
{% endif %}

{# Load from Snowflake TABLE_METADATA #}
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

{% set results = run_query(query) %}

{% set config_dict = {} %}
{% if execute %}
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
{% endif %}

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
