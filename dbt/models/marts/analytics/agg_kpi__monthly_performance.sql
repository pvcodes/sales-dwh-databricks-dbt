{{
    config(
        materialized="table",
        tags=["analytics", "kpi"]
    )
}}

-- Headline KPI tile for the executive dashboard: one row per reporting month,
-- with the current period compared against the prior month and the same month a
-- year earlier.
--
-- Grain: one row per `order_year_month`.

with sales as (

    select * from {{ ref('fct_sales') }}

    where order_date is not null

),

monthly as (

    select
        order_year_month,
        min(order_year)                                       as report_year,
        min(order_month_number)                               as report_month_number,
        min(order_month_name)                                 as report_month_name,
        min(order_year_quarter)                               as report_quarter,
        count(distinct order_number)                          as order_count,
        count(distinct customer_sk)                           as active_customers,
        sum(quantity)                                         as units_sold,
        sum(sales_amount)                                     as gross_revenue,
        sum(discount_amount)                                  as discount_amount,
        sum(gross_margin)                                     as gross_margin,
        sum(quantity * product_cost)                          as cost_of_goods,
        {{ null_safe_divide('sum(days_to_ship)', 'count(*)') }}  as avg_days_to_ship,
        {{ null_safe_divide('sum(days_to_due)', 'count(*)') }}   as avg_days_to_due
    from sales
    group by 1

),

with_trends as (

    select
        *,
        lag(gross_revenue) over w                          as prior_month_revenue,
        lag(gross_revenue, 12) over w                     as prior_year_revenue
    from monthly
    window w as (order by order_year_month)

)

select
    order_year_month,
    report_year,
    report_month_number,
    report_month_name,
    report_quarter,
    order_count,
    active_customers,
    units_sold,
    gross_revenue,
    cost_of_goods,
    discount_amount,
    gross_margin,
    case
        when gross_revenue = 0 then null
        else round(gross_margin / gross_revenue, 4)
    end                                                 as gross_margin_pct,
    round(gross_revenue / nullif(order_count, 0), 2)     as average_order_value,
    round(gross_revenue / nullif(active_customers, 0), 2) as revenue_per_customer,
    round(gross_revenue / nullif(units_sold, 0), 2)       as average_unit_price,
    avg_days_to_ship,
    avg_days_to_due,
    prior_month_revenue,
    prior_year_revenue,
    case
        when prior_month_revenue is null or prior_month_revenue = 0 then null
        else round((gross_revenue - prior_month_revenue) / prior_month_revenue, 4)
    end                                                 as revenue_mom_pct,
    case
        when prior_year_revenue is null or prior_year_revenue = 0 then null
        else round((gross_revenue - prior_year_revenue) / prior_year_revenue, 4)
    end                                                 as revenue_yoy_pct
from with_trends
order by order_year_month
