{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="sales_line_id",
        on_schema_change="sync_all_columns",
        tags=["facts", "incremental"],
        **sales_dwh_incremental_config(['order_date'])
    )
}}

-- Sales fact table at order-line grain, in Kimball star form.
--
-- Incremental strategy: the model is rebuilt for a short lookback window
-- (`var('fact_lookback_days')`) on every run rather than being appended, because
-- late-arriving orders and restatements in the CRM would otherwise leave stale
-- rows in the fact. The merge is keyed on the natural order-line key.
--
-- Grain: one row per `order_number` + `product_key`.

with enriched as (

    select * from {{ ref('int_sales__enriched') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

products as (

    select * from {{ ref('dim_products') }}

    where is_current

),

{% if is_incremental() %}

-- Only re-process the lookback window on an incremental run. The window is
-- widened rather than made exact because the fact is corrected, not extended.
incremental_filter as (

    select *
    from enriched
    where order_date >= (
        select coalesce(max(order_date), date '1900-01-01') - {{ var('fact_lookback_days') }}
        from {{ this }}
    )

),

{% else %}

incremental_filter as (

    select * from enriched

),

{% endif %}

joined as (

    select
        incremental_filter.sales_line_id,
        -- Degenerate dimension: the order number is the grain of the fact, so it
        -- lives on the fact rather than in a dimension of its own.
        incremental_filter.order_number,
        -- Natural keys travel with the surrogate keys so the fact is
        -- self-describing: it can be reconciled against the source and joined
        -- without first resolving the surrogate back through a dimension.
        customers.customer_sk,
        customers.customer_id,
        customers.customer_key,
        products.product_sk,
        incremental_filter.product_key,
        incremental_filter.product_version_id,
        {{ date_dim_attrs('incremental_filter.order_date') }},
        incremental_filter.ship_date,
        incremental_filter.due_date,
        incremental_filter.days_to_ship,
        incremental_filter.days_to_due,
        incremental_filter.days_in_transit,
        incremental_filter.sales_amount,
        incremental_filter.quantity,
        incremental_filter.unit_price,
        incremental_filter.discount_amount,
        incremental_filter.realised_unit_price,
        incremental_filter.product_cost,
        incremental_filter.gross_margin,
        incremental_filter.gross_margin_pct,
        incremental_filter.unit_margin_pct,
        incremental_filter.product_name,
        incremental_filter.product_line,
        incremental_filter.product_version_count,
        incremental_filter.category_id,
        incremental_filter.category_name,
        incremental_filter.subcategory_name,
        incremental_filter.requires_maintenance,
        incremental_filter.customer_name,
        incremental_filter.customer_gender,
        incremental_filter.customer_marital_status,
        incremental_filter.customer_age_band,
        incremental_filter.customer_country,
        incremental_filter.bronze_ingested_at
    from incremental_filter
    left join customers
        on incremental_filter.customer_id = customers.customer_id
    left join products
        on incremental_filter.product_key = products.product_key

)

select * from joined
