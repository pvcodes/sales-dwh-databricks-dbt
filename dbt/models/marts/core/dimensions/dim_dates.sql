{{
    config(
        materialized="table",
        **sales_dwh_table_config(['date_key'])
    )
}}

-- Date dimension covering the reporting horizon.
--
-- The spine is generated with `date_spine` rather than derived from the fact, so
-- periods with no orders still appear in reports and trend charts. Grain: one
-- row per calendar day.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('reporting_start_date') ~ "' as date)",
        end_date="cast('" ~ var('reporting_end_date') ~ "' as date)"
    ) }}

)

select
    cast(date_day as date)                                               as date_key,
    year(date_day)                                                      as date_year,
    quarter(date_day)                                                   as date_quarter,
    month(date_day)                                                     as date_month_number,
    {{ month_name('month(date_day)') }}                                 as date_month_name,
    date_trunc('month', date_day)                                       as date_month_start,
    dayofyear(date_day)                                                 as date_day_of_year,
    {{ date_part('day_of_week_sun_1', 'date_day') }}                    as date_day_of_week,
    {{ date_part('week_of_year', 'date_day') }}                         as date_week_of_year,
    {{ date_part('day_of_month', 'date_day') }}                         as date_day_of_month,
    {{ day_name_expr('date_day') }}                                     as date_day_name,
    concat(cast(year(date_day) as varchar), '-Q',
           cast(quarter(date_day) as varchar))                          as date_year_quarter,
    concat(cast(year(date_day) as varchar), '-',
           lpad(cast(month(date_day) as varchar), 2, '0'))              as date_year_month,
    (year(date_day) * 100) + quarter(date_day)                          as date_year_quarter_key,
    (year(date_day) * 100) + month(date_day)                            as date_year_month_key,
    {{ date_diff_days('date_day', current_date_expr()) }}               as days_ago,
    date_day < {{ current_date_expr() }}                                as is_past,
    date_day = {{ current_date_expr() }}                                as is_today,
    case
        when {{ day_name_expr('date_day') }} in ('Saturday', 'Sunday') then true
        else false
    end                                                                 as is_weekend
from spine
