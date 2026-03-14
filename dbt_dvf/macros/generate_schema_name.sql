{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override dbt's default schema name generation.

        When a model specifies a custom schema (e.g., schema='dvf_analytics'),
        use that schema name directly instead of prefixing it with the
        target schema (e.g., dvf_staging_dvf_analytics).

        This ensures:
          - Staging/intermediate models -> dvf_staging (default target dataset)
          - Mart models with schema='dvf_analytics' -> dvf_analytics
          - Seeds with schema='dvf_raw' -> dvf_raw
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
