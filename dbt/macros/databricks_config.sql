{#-
    Databricks-only table settings.

    `dbt_project.yml` cannot host conditional blocks (it is rendered before it
    is parsed as YAML), so Delta/Liquid Clustering settings are applied per
    model through `sales_dwh_table_config()`. On any other adapter the macro
    expands to an empty dict, which keeps the same models runnable on DuckDB.
-#}

{% macro sales_dwh_table_config(cluster_by=none) -%}

    {%- if target.type in ('databricks', 'spark') -%}
        {%- set cfg = {'file_format': 'delta'} -%}
        {%- if cluster_by is not none and cluster_by | length > 0 -%}
            {%- do cfg.update({'liquid_clustered_by': cluster_by}) -%}
        {%- endif -%}
        {{- return(cfg) -}}
    {%- else -%}
        {{- return({}) -}}
    {%- endif -%}

{%- endmacro %}


{% macro sales_dwh_incremental_config(cluster_by=none) -%}

    {%- if target.type in ('databricks', 'spark') -%}
        {%- set cfg = {
            'file_format': 'delta',
            'liquid_clustered_by': cluster_by if cluster_by else ['order_date'],
            'auto_liquid_cluster': true
        } -%}
        {{- return(cfg) -}}
    {%- else -%}
        {{- return({}) -}}
    {%- endif -%}

{%- endmacro %}
