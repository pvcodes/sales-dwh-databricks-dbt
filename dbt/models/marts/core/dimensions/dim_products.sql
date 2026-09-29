{{
    config(
        materialized="table",
        **sales_dwh_table_config(['product_key', 'category_name', 'is_current'])
    )
}}

-- Product dimension (SCD type 2).
--
-- The CRM product master is naturally versioned: the same `product_key` reappears
-- with a new `prd_start_dt` whenever its cost or lifecycle window changes. That
-- is modelled as a real type 2 dimension rather than collapsed to one row.
--
-- The validity window is rebuilt from the version start dates with `lead()`
-- instead of trusting `prd_end_dt`, because the source stores the *previous*
-- version's end date and leaves it earlier than `prd_start_dt` for open items.
--
-- Grain: one row per product version.

with product_versions as (

    select * from {{ ref('stg_crm__products') }}

    where product_key is not null
      and start_date is not null

),

categories as (

    select * from {{ ref('stg_erp__product_categories') }}

),

sequenced as (

    select
        product_key,
        product_version_id,
        product_key_qualified,
        category_id,
        product_name,
        product_cost,
        product_line,
        start_date,
        effective_end_date,
        lead(start_date) over (
            partition by product_key
            order by start_date, product_version_id
        )                                                     as next_version_start_date,
        row_number() over (
            partition by product_key
            order by start_date, product_version_id
        )                                                     as version_number,
        count(*) over (partition by product_key)              as version_count
    from product_versions

),

validity as (

    select
        sequenced.*,
        start_date                                             as valid_from_date,
        case
            when next_version_start_date is null then {{ open_ended_date() }}
            else {{ subtract_days('next_version_start_date', 1) }}
        end                                                   as valid_to_date,
        next_version_start_date is null                       as is_current,
        product_cost is not null                              as has_product_cost
    from sequenced

)

select
    md5(concat('prod|', validity.product_key, '|', cast(validity.valid_from_date as varchar)))  as product_sk,
    validity.product_key,
    validity.product_version_id,
    validity.product_key_qualified,
    validity.category_id,
    categories.category_name,
    categories.subcategory_name,
    categories.requires_maintenance,
    validity.product_name,
    validity.product_cost,
    validity.has_product_cost,
    validity.product_line,
    validity.valid_from_date,
    validity.valid_to_date,
    validity.is_current,
    validity.version_number,
    validity.version_count,
    {{ date_diff_days('validity.valid_from_date', 'validity.valid_to_date') }}             as valid_for_days,
    concat(validity.product_line, ' / ', categories.category_name)                           as product_line_category,
    concat(validity.category_id, ' | ', categories.category_name)                           as category_label
from validity
left join categories on validity.category_id = categories.category_id
