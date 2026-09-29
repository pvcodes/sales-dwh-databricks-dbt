{#-
    Adapter-portable scalar helpers.

    The project is built on Databricks/Spark SQL in production and exercised on
    DuckDB in CI, and the two disagree on both date parsing and `datediff`
    signatures. Every model routes these through a macro instead of hard-coding
    a dialect.
-#}

{% macro date_diff_days(start_expr, end_expr) -%}
    {%- if target.type in ('databricks', 'spark') -%}
        datediff({{ end_expr }}, {{ start_expr }})
    {%- else -%}
        datediff('day', {{ start_expr }}, {{ end_expr }})
    {%- endif -%}
{%- endmacro %}


{% macro parse_compact_date(expr) -%}
    {#- Parse a bare `yyyyMMdd` string, returning null instead of failing. -#}
    {%- if target.type == 'duckdb' -%}
        try_cast(try_strptime({{ expr }}, '%Y%m%d') as date)
    {%- elif target.type in ('databricks', 'spark') -%}
        try_to_date({{ expr }}, 'yyyyMMdd')
    {%- else -%}
        try_cast(to_date({{ expr }}, 'YYYYMMDD') as date)
    {%- endif -%}
{%- endmacro %}


{% macro null_safe_divide(numerator_expr, denominator_expr) -%}
    {#- Division that yields null rather than an error/NaN on a zero divisor. -#}
    case
        when {{ denominator_expr }} is null or {{ denominator_expr }} = 0 then null
        else {{ numerator_expr }} / {{ denominator_expr }}
    end
{%- endmacro %}


{% macro current_timestamp_utc() -%}
    {%- if target.type in ('databricks', 'spark') -%}
        current_timestamp()
    {%- else -%}
        current_timestamp
    {%- endif -%}
{%- endmacro %}


{% macro open_ended_date() -%}
    {#- Sentinel used to represent "still current" for SCD type 2 dimensions. -#}
    date '9999-12-31'
{%- endmacro %}


{% macro current_date_expr() -%}
    {%- if target.type in ('databricks', 'spark') -%}
        current_date()
    {%- else -%}
        current_date
    {%- endif -%}
{%- endmacro %}


{% macro day_of_month_expr(expr) -%}
    {%- if target.type in ('databricks', 'spark') -%}
        dayofmonth({{ expr }})
    {%- else -%}
        extract(day from {{ expr }})
    {%- endif -%}
{%- endmacro %}


{% macro full_years_between(start_expr, end_expr) -%}
    {#-
        Completed years between two dates. Neither adapter offers a portable
        `date_diff` for calendar years, so it is derived from the parts.
    -#}
    case
        when {{ start_expr }} is null or {{ end_expr }} is null then null
        else (
            extract(year from {{ end_expr }})
            - extract(year from {{ start_expr }})
            - case
                when extract(month from {{ end_expr }}) < extract(month from {{ start_expr }})
                    then 1
                when extract(month from {{ end_expr }}) = extract(month from {{ start_expr }})
                     and {{ day_of_month_expr(end_expr) }} < {{ day_of_month_expr(start_expr) }}
                    then 1
                else 0
              end
        )
    end
{%- endmacro %}


{% macro month_name(month_expr) -%}
    {#- 1 -> 'Jan' ... 12 -> 'Dec', portable across adapters. -#}
    case cast({{ month_expr }} as integer)
        when 1  then 'Jan' when 2  then 'Feb' when 3  then 'Mar'
        when 4  then 'Apr' when 5  then 'May' when 6  then 'Jun'
        when 7  then 'Jul' when 8  then 'Aug' when 9  then 'Sep'
        when 10 then 'Oct' when 11 then 'Nov' when 12 then 'Dec'
        else 'Unknown'
    end
{%- endmacro %}


{% macro subtract_days(expr, days) -%}
    {%- if target.type in ('databricks', 'spark') -%}
        date_sub({{ expr }}, {{ days }})
    {%- else -%}
        ({{ expr }} - {{ days }})
    {%- endif -%}
{%- endmacro %}


{% macro date_part(part, expr) -%}
    {#-
        Portable single date part. Spark's `dayofweek` is 1=Sunday and its
        `dayofmonth` has no DuckDB equivalent, so the risky parts are
        normalised here rather than in the models.
        -#}
    {%- if target.type in ('databricks', 'spark') -%}
        {%- if part == 'day_of_month' -%}
            dayofmonth({{ expr }})
        {%- elif part == 'day_of_week_sun_1' -%}
            dayofweek({{ expr }})
        {%- elif part == 'week_of_year' -%}
            weekofyear({{ expr }})
        {%- else -%}
            {{ part }}({{ expr }})
        {%- endif -%}
    {%- else -%}
        {%- if part == 'day_of_week_sun_1' -%}
            extract(dow from {{ expr }})
        {%- elif part == 'day_of_month' -%}
            extract(day from {{ expr }})
        {%- elif part == 'week_of_year' -%}
            extract(week from {{ expr }})
        {%- else -%}
            extract({{ part }} from {{ expr }})
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}


{% macro day_name_expr(expr) -%}
    {%- if target.type in ('databricks', 'spark') -%}
        case dayofweek({{ expr }})
            when 1 then 'Sunday' when 2 then 'Monday' when 3 then 'Tuesday'
            when 4 then 'Wednesday' when 5 then 'Thursday' when 6 then 'Friday'
            when 7 then 'Saturday'
        end
    {%- else -%}
        case extract(dow from {{ expr }})
            when 0 then 'Sunday' when 1 then 'Monday' when 2 then 'Tuesday'
            when 3 then 'Wednesday' when 4 then 'Thursday' when 5 then 'Friday'
            when 6 then 'Saturday'
        end
    {%- endif -%}
{%- endmacro %}


{% macro date_dim_attrs(date_expr) -%}
    {#-
        Standard calendar attributes denormalised onto the fact so the common
        BI questions (trend, quarter, day of week) need no join to the date
        dimension.
    -#}
        cast({{ date_expr }} as date)                          as order_date,
        year({{ date_expr }})                                  as order_year,
        quarter({{ date_expr }})                               as order_quarter,
        month({{ date_expr }})                                 as order_month_number,
        {{ month_name('month(' ~ date_expr ~ ')') }}           as order_month_name,
        date_trunc('month', {{ date_expr }})                   as order_month_start,
        dayofyear({{ date_expr }})                             as order_day_of_year,
        {{ date_part('day_of_week_sun_1', date_expr) }}        as order_day_of_week,
        {{ date_part('week_of_year', date_expr) }}             as order_week_of_year,
        {{ date_part('day_of_month', date_expr) }}             as order_day_of_month,
        {{ day_name_expr(date_expr) }}                         as order_day_name,
        concat(
            cast(year({{ date_expr }}) as varchar), '-Q',
            cast(quarter({{ date_expr }}) as varchar)
        )                                                      as order_year_quarter,
        concat(
            cast(year({{ date_expr }}) as varchar), '-',
            lpad(cast(month({{ date_expr }}) as varchar), 2, '0')
        )                                                      as order_year_month
{%- endmacro %}
