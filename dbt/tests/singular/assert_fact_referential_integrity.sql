-- Every fact row must resolve to a customer and to a product version.
--
-- Referential integrity is enforced with a `left join` so that an unmatched
-- dimension row shows up as a null key rather than silently dropping revenue.
-- This test is what turns that silent drop into a build failure.

with violations as (

    select
        'missing_customer' as failure_reason,
        count(*)            as offending_rows
    from {{ ref('fct_sales') }}
    where customer_sk is null

    union all

    select
        'missing_product',
        count(*)
    from {{ ref('fct_sales') }}
    where product_sk is null

    union all

    select
        'duplicate_line_key',
        count(*)
    from (
        select sales_line_id
        from {{ ref('fct_sales') }}
        group by sales_line_id
        having count(*) > 1
    ) duplicates

)

select *
from violations
where offending_rows > 0
