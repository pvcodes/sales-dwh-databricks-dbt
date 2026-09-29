{{
    config(
        materialized="table",
        tags=["analytics", "category"]
    )
}}

-- Category rollup: the product hierarchy summarised for range and category
-- reviews. Grain: one row per category, subcategory, product line and
-- maintenance flag.
--
-- Distinct counts are taken at the true category grain in a separate CTE.
-- Aggregating them from a per-product rollup would over-count, because a single
-- order that spans two products in the same category would be counted twice.

with sales as (

    select * from {{ ref('fct_sales') }}

),

coalesced as (

    select
        -- Unclassifiable products stay visible under an explicit bucket rather
        -- than being dropped by the join to the dimension.
        coalesce(category_id, 'UNCLASSIFIED')                     as category_id,
        coalesce(category_name, 'Unclassified')                    as category_name,
        coalesce(subcategory_name, 'Unclassified')                 as subcategory_name,
        coalesce(product_line, 'Other')                            as product_line,
        coalesce(requires_maintenance, false)                      as requires_maintenance,
        product_key,
        order_number,
        customer_sk,
        quantity,
        sales_amount,
        gross_margin
    from sales

),

category_distincts as (

    select
        category_id,
        subcategory_name,
        product_line,
        requires_maintenance,
        count(distinct order_number)    as order_count,
        count(distinct customer_sk)     as buyer_count,
        count(distinct product_key)     as product_count
    from coalesced
    group by 1, 2, 3, 4

),

category_measures as (

    select
        category_id,
        max(category_name)              as category_name,
        subcategory_name,
        product_line,
        requires_maintenance,
        count(*)                        as order_line_count,
        sum(quantity)                   as units_sold,
        sum(sales_amount)               as gross_revenue,
        sum(gross_margin)               as gross_margin
    from coalesced
    group by 1, 3, 4, 5

),

totals as (

    select
        sum(gross_revenue)              as portfolio_revenue,
        sum(gross_margin)               as portfolio_gross_margin,
        sum(units_sold)                 as portfolio_units
    from category_measures

)

select
    measures.category_id,
    measures.category_name,
    measures.subcategory_name,
    measures.product_line,
    measures.requires_maintenance,
    distincts.order_count,
    distincts.buyer_count,
    distincts.product_count,
    measures.order_line_count,
    measures.units_sold,
    measures.gross_revenue,
    measures.gross_margin,
    case
        when measures.gross_revenue = 0 then null
        else round(measures.gross_margin / measures.gross_revenue, 4)
    end                                                     as gross_margin_pct,
    round(measures.gross_revenue / nullif(measures.units_sold, 0), 2) as average_unit_price,
    round(measures.gross_revenue / nullif(distincts.order_count, 0), 2) as average_order_value,
    round(measures.gross_revenue / nullif(totals.portfolio_revenue, 0), 4) as revenue_share,
    case
        when measures.gross_margin = 0 then null
        else round(measures.gross_margin / nullif(totals.portfolio_gross_margin, 0), 4)
    end                                                     as margin_share
from category_measures as measures
inner join category_distincts as distincts
    on  measures.category_id          = distincts.category_id
    and measures.subcategory_name    = distincts.subcategory_name
    and measures.product_line         = distincts.product_line
    and measures.requires_maintenance = distincts.requires_maintenance
cross join totals
