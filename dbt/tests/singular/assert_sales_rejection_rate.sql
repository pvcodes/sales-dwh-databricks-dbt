-- Bounds how much of the sales source the silver layer is allowed to reject.
--
-- 25 lines are dropped for non-positive measures and 18 more for unparseable
-- dates, which is 0.07% of the extract. A sudden jump means the upstream feed
-- changed shape rather than that the data got dirtier, so the test fails
-- rather than warns: an unnoticed change in rejection rate is how a broken feed
-- turns into a quietly wrong dashboard.

with source_rows as (

    select count(*) as row_count
    from {{ source('bronze', 'crm_sales_details') }}

),

retained_rows as (

    select count(*) as row_count
    from {{ ref('stg_crm__sales_details') }}

),

reconciliation as (

    select
        source_rows.row_count as source_row_count,
        retained_rows.row_count as retained_row_count,
        source_rows.row_count - retained_rows.row_count as rejected_row_count,
        (
            cast(retained_rows.row_count as double) / nullif(source_rows.row_count, 0)
        ) as retention_rate
    from source_rows
    cross join retained_rows

)

select *
from reconciliation
where retention_rate < {{ var('max_sales_rejection_rate') }}
