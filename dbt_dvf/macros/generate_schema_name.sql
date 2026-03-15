/*
    generate_schema_name — Override dbt's default BigQuery dataset routing.

    By default, dbt prepends the target schema (dataset) name to any custom
    schema, producing names like "dvf_staging_dvf_analytics". This macro
    removes that prefix behavior so that custom schemas map directly to
    BigQuery datasets:

      Model config                   -> BigQuery dataset
      --------------------------------  ----------------
      (no custom schema)              -> dvf_staging   (target default)
      schema='dvf_analytics'          -> dvf_analytics
      schema='dvf_raw'                -> dvf_raw

    Parameters:
      custom_schema_name  STRING or NONE  The schema value from config(), if any
      node                OBJECT          The dbt node (model/seed/snapshot)
*/
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
