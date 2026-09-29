{{
    config(
        materialized="view"
    )
}}

-- Source: CRM sales order lines. Grain: one row per order line.
--
-- Rejected here (never reaches the fact table):
--   * rows with a non-positive or unparseable quantity / price / sales amount
--   * rows whose `sls_order_dt` is not a valid `yyyyMMdd` date
-- The rejected count is asserted by `tests/assert_sales_rejection_rate.sql`.

with source as (

    select * from {{ source('bronze', 'crm_sales_details') }}

),

renamed as (

    select
        sls_ord_num                                         as order_number_raw,
        sls_prd_key                                         as product_key_raw,
        sls_cust_id                                         as customer_id_raw,
        sls_order_dt                                        as order_date_raw,
        sls_ship_dt                                         as ship_date_raw,
        sls_due_dt                                          as due_date_raw,
        sls_sales                                           as sales_amount_raw,
        sls_quantity                                        as quantity_raw,
        sls_price                                           as price_raw,
        _ingested_at                                        as bronze_ingested_at,
        _source_file                                        as bronze_source_file,
        _batch_id                                           as bronze_batch_id
    from source

),

typed as (

    select
        nullif(upper(trim(order_number_raw)), '')                        as order_number,
        nullif(upper(trim(product_key_raw)), '')                         as product_key,
        try_cast(nullif(trim(customer_id_raw), '') as integer)            as customer_id,
        nullif(trim(order_date_raw), '')                                 as _order_date_text,
        nullif(trim(ship_date_raw), '')                                  as _ship_date_text,
        nullif(trim(due_date_raw), '')                                   as _due_date_text,
        try_cast(nullif(trim(sales_amount_raw), '') as decimal(18, 2))    as sales_amount,
        try_cast(nullif(trim(quantity_raw), '') as integer)               as quantity,
        try_cast(nullif(trim(price_raw), '') as decimal(18, 2))           as unit_price,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from renamed

),

-- `yyyyMMdd` arrives as a bare integer, so it is normalised to text first and
-- then parsed. Corrupt values (0, 5489, ...) fail the parse and become null.
parsed_dates as (

    select
        order_number,
        product_key,
        customer_id,
        {{ parse_compact_date('_order_date_text') }} as order_date,
        {{ parse_compact_date('_ship_date_text') }}  as ship_date,
        {{ parse_compact_date('_due_date_text') }}   as due_date,
        sales_amount,
        quantity,
        unit_price,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from typed
    where _order_date_text is not null
      and _ship_date_text  is not null
      and _due_date_text   is not null

),

cleaned as (

    select
        order_number,
        product_key,
        customer_id,
        order_date,
        ship_date,
        due_date,
        sales_amount,
        quantity,
        unit_price,
        -- Fulfilment latency, the two measures the business actually tracks.
        {{ date_diff_days('order_date', 'ship_date') }}  as days_to_ship,
        {{ date_diff_days('order_date', 'due_date') }}   as days_to_due,
        {{ date_diff_days('ship_date', 'due_date') }}    as days_in_transit,
        sales_amount - (quantity * unit_price)  as discount_amount,
        case
            when quantity > 0 and unit_price > 0
                then round(sales_amount / quantity, 2)
        end                                    as realised_unit_price,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from parsed_dates
    where sales_amount is not null and sales_amount > 0
      and quantity     is not null and quantity     > 0
      and unit_price   is not null and unit_price   > 0
      and order_number is not null
      and product_key  is not null
      and customer_id  is not null
      -- A line with an unparseable date cannot be placed on the timeline, and
      -- every downstream measure (trend, duration, date attributes) depends on
      -- it, so the whole line is rejected rather than kept with a null date.
      and order_date is not null
      and ship_date  is not null
      and due_date   is not null
      and ship_date  >= order_date
      and due_date   >= ship_date

)

select * from cleaned
