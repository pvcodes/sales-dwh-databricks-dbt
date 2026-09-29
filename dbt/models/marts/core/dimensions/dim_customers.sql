{{
    config(
        materialized="table",
        **sales_dwh_table_config(['customer_key', 'country'])
    )
}}

-- Customer dimension (SCD type 1).
--
-- The natural key is the CRM `customer_key`; `customer_sk` is a warehouse
-- surrogate key. A hash of the natural key is used so the surrogate is stable
-- across full rebuilds and does not shift when unrelated rows are inserted.
--
-- Grain: one row per customer.

with customers as (

    select * from {{ ref('int_customers__conformed') }}

),

order_rollup as (

    select
        sales.customer_id,
        count(*)                                       as order_line_count,
        count(distinct sales.order_number)             as order_count,
        sum(sales.quantity)                            as total_units,
        sum(sales.sales_amount)                        as lifetime_sales_amount,
        sum(sales.gross_margin)                        as lifetime_gross_margin,
        min(sales.order_date)                          as first_order_date,
        max(sales.order_date)                          as last_order_date
    from {{ ref('int_sales__enriched') }} as sales
    group by 1

),

final as (

    select
        md5(concat('cust|', cast(customer_key as varchar)))    as customer_sk,
        customers.customer_id,
        customers.customer_key,
        customer_first_name,
        customer_last_name,
        customer_name,
        gender,
        marital_status,
        birth_date,
        current_age,
        age_band,
        country,
        customer_created_date,
        coalesce(order_rollup.order_line_count, 0)     as order_line_count,
        coalesce(order_rollup.order_count, 0)         as order_count,
        coalesce(order_rollup.total_units, 0)         as total_units,
        coalesce(order_rollup.lifetime_sales_amount, 0) as lifetime_sales_amount,
        order_rollup.lifetime_gross_margin            as lifetime_gross_margin,
        order_rollup.first_order_date,
        order_rollup.last_order_date,
        {{ date_diff_days('order_rollup.first_order_date', current_date_expr()) }} as days_since_first_order,
        {{ date_diff_days('order_rollup.last_order_date', current_date_expr()) }}  as days_since_last_order,
        order_rollup.first_order_date is not null      as is_customer,
        true                                           as is_current
    from customers
    left join order_rollup on customers.customer_id = order_rollup.customer_id

)

select * from final
