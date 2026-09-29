{#-
    Medallion schemas must land exactly on `bronze` / `silver` / `gold`.
    dbt's default macro concatenates the target schema with the custom one
    (`bronze_silver`), which breaks the Unity Catalog layout this project
    depends on, so the custom schema is used verbatim.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim if custom_schema_name else default_schema }}
    {%- endif -%}

{%- endmacro %}
