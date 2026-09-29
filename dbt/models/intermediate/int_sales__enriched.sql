{{
    config(
        materialized="view"
    )
}}

-- Order lines joined to the conformed customer and to the single current
-- version of each product, with the ERP category hierarchy rolled up.
--
-- Grain: one row per sales order line. The `product_key` chosen here is the
-- *current* product version (see `int_products__current_version`), so the fact
-- never fans out against the versioned product master.

with sales as (

    select * from {{ ref('stg_crm__sales_details') }}

),

customers as (

    select * from {{ ref('int_customers__conformed') }}

),

products as (

    select * from {{ ref('int_products__current_version') }}

),

categories as (

    select * from {{ ref('stg_erp__product_categories') }}

),

joined as (

    select
        sales.order_number,
        sales.product_key,
        customers.customer_id,
        customers.customer_key,
        customers.customer_name,
        customers.gender                                        as customer_gender,
        customers.marital_status                                as customer_marital_status,
        customers.age_band                                      as customer_age_band,
        customers.country                                       as customer_country,
        sales.order_date,
        sales.ship_date,
        sales.due_date,
        sales.days_to_ship,
        sales.days_to_due,
        sales.days_in_transit,
        sales.sales_amount,
        sales.quantity,
        sales.unit_price,
        sales.discount_amount,
        sales.realised_unit_price,
        products.product_version_id,
        products.product_name,
        products.product_cost,
        products.product_line,
        products.product_version_count,
        products.product_start_date,
        products.effective_end_date                            as product_end_date,
        categories.category_id,
        categories.category_name,
        categories.subcategory_name,
        categories.requires_maintenance,
        sales.bronze_ingested_at
    from sales
    left join customers  on sales.customer_id = customers.customer_id
    left join products   on sales.product_key  = products.product_key
    left join categories on products.category_id = categories.category_id

),

enriched as (

    select
        *,
        -- Gross margin is only meaningful where the product has a cost on file.
        case
            when product_cost is null then null
            else sales_amount - (quantity * product_cost)
        end                                       as gross_margin,
        case
            when product_cost is null or product_cost = 0 then null
            else round(
                (sales_amount - (quantity * product_cost)) / sales_amount, 4
            )
        end                                       as gross_margin_pct,
        case
            when unit_price > 0 then (unit_price - product_cost) / unit_price
        end                                       as unit_margin_pct
    from joined

)

select
    -- Business key of the order line, stable across reloads.
    concat(order_number, '-', product_key)                                    as sales_line_id,
    order_number,
    product_key,
    customer_id,
    customer_key,
    product_version_id,
    order_date,
    ship_date,
    due_date,
    days_to_ship,
    days_to_due,
    days_in_transit,
    sales_amount,
    quantity,
    unit_price,
    discount_amount,
    realised_unit_price,
    product_cost,
    gross_margin,
    gross_margin_pct,
    unit_margin_pct,
    product_name,
    product_line,
    product_version_count,
    product_start_date,
    product_end_date,
    category_id,
    category_name,
    subcategory_name,
    requires_maintenance,
    customer_name,
    customer_gender,
    customer_marital_status,
    customer_age_band,
    customer_country,
    bronze_ingested_at
from enriched
