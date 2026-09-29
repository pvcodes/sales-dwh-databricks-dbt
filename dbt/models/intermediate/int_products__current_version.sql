{{
    config(
        materialized="view"
    )
}}

-- Resolves the CRM product master down to exactly one row per product key.
--
-- `crm_prd_info` holds one row per product *version* and 77 of the 295 product
-- keys are versioned. Joining `crm_sales_details` straight to the master
-- therefore multiplies the fact table (60,398 -> 89,818 rows in this dataset).
--
-- Selection rule, applied in priority order:
--   1. prefer the open-ended (current) version
--   2. then the latest `start_date`
--   3. then the highest `product_version_id` as a final tie-break
-- Rule 3 guarantees the result is deterministic, which matters because a
-- non-deterministic pick would make the gold marts unstable between runs.

with product_versions as (

    select * from {{ ref('stg_crm__products') }}

),

ranked as (

    select
        product_key,
        product_version_id,
        product_name,
        product_cost,
        product_line,
        category_id,
        category_key_prefix,
        product_key_qualified,
        start_date,
        end_date,
        effective_end_date,
        is_current_version,
        bronze_ingested_at,
        row_number() over (
            partition by product_key
            order by
                is_current_version desc,
                start_date desc,
                product_version_id desc
        ) as version_rank,
        count(*) over (partition by product_key) as version_count
    from product_versions
    where product_key is not null

)

select
    product_key,
    product_version_id                                        as product_version_id,
    product_name,
    product_cost,
    product_line,
    category_id,
    category_key_prefix,
    product_key_qualified,
    start_date                                                 as product_start_date,
    end_date                                                   as product_end_date,
    effective_end_date,
    is_current_version,
    version_count                                              as product_version_count,
    {{ date_diff_days('start_date', 'effective_end_date') }}  as product_lifecycle_days,
    bronze_ingested_at
from ranked
where version_rank = 1
