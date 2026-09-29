{{
    config(
        materialized="table",
        tags=["analytics", "product"]
    )
}}

-- Product performance with an ABC classification.
--
-- Products are ranked by lifetime revenue and split into A/B/C bands where A
-- carries roughly the top 80% of revenue. This is the table merchandising and
-- range reviews are run from.
--
-- Grain: one row per product.

with sales as (

    select * from {{ ref('fct_sales') }}

),

current_products as (

    select
        product_sk,
        product_key,
        product_name,
        product_line,
        category_id,
        category_name,
        subcategory_name,
        product_cost
    from {{ ref('dim_products') }}

    where is_current

),

per_product as (

    select
        sales.product_key,
        count(distinct sales.order_number)              as order_count,
        count(distinct sales.customer_sk)               as buyer_count,
        sum(sales.quantity)                             as units_sold,
        sum(sales.sales_amount)                         as gross_revenue,
        sum(sales.gross_margin)                         as gross_margin,
        sum(sales.discount_amount)                      as discount_amount,
        min(sales.order_date)                           as first_order_date,
        max(sales.order_date)                           as last_order_date
    from sales
    group by 1

),

ranked as (

    select
        per_product.*,
        sum(gross_revenue) over ()                      as portfolio_revenue,
        row_number() over (order by gross_revenue desc) as revenue_rank,
        dense_rank() over (order by gross_revenue desc) as revenue_tiebreak
    from per_product

)

select
    ranked.product_key,
    current_products.product_sk,
    current_products.product_name,
    current_products.product_line,
    current_products.category_id,
    current_products.category_name,
    current_products.subcategory_name,
    current_products.product_cost,
    ranked.revenue_rank,
    ranked.revenue_tiebreak,
    ranked.order_count,
    ranked.buyer_count,
    ranked.units_sold,
    ranked.gross_revenue,
    ranked.gross_margin,
    ranked.discount_amount,
    case
        when ranked.gross_revenue = 0 then null
        else round(ranked.gross_margin / ranked.gross_revenue, 4)
    end                                                 as gross_margin_pct,
    round(ranked.gross_revenue / nullif(ranked.units_sold, 0), 2) as average_unit_price,
    round(ranked.gross_revenue / nullif(ranked.order_count, 0), 2) as average_order_value,
    first_order_date,
    last_order_date,
    -- Cumulative share of portfolio revenue, used to cut the A/B/C bands.
    round(
        sum(ranked.gross_revenue) over (order by ranked.revenue_rank)
        / nullif(sum(ranked.gross_revenue) over (), 0),
        4
    )                                                   as cumulative_revenue_share,
    case
        when sum(ranked.gross_revenue) over (order by ranked.revenue_rank)
             / nullif(sum(ranked.gross_revenue) over (), 0) <= 0.80 then 'A'
        when sum(ranked.gross_revenue) over (order by ranked.revenue_rank)
             / nullif(sum(ranked.gross_revenue) over (), 0) <= 0.95 then 'B'
        else 'C'
    end                                                 as abc_class
from ranked
inner join current_products on ranked.product_key = current_products.product_key
