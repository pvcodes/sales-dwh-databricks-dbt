-- The fact table must reconcile to the cleansed sales layer with no fan-out.
--
-- This is the single most valuable test in the project. The CRM product master
-- is versioned, so joining sales to it naively inflates 60,373 order lines to
-- 89,818. That bug produces a plausible-looking warehouse that is simply wrong
-- in a way no column-level test would catch, so it is asserted directly.

with fact as (

    select
        count(*)                                   as fact_row_count,
        count(distinct sales_line_id)              as distinct_line_count,
        sum(sales_amount)                         as fact_revenue,
        sum(quantity)                             as fact_units
    from {{ ref('fct_sales') }}

),

expected as (

    select
        count(*)                                   as expected_row_count,
        sum(sales_amount)                         as expected_revenue,
        sum(quantity)                             as expected_units
    from {{ ref('stg_crm__sales_details') }}

)

select
    fact.fact_row_count,
    expected.expected_row_count,
    fact.fact_revenue,
    expected.expected_revenue,
    fact.fact_units,
    expected.expected_units
from fact
cross join expected
where fact.fact_row_count <> expected.expected_row_count
   or fact.distinct_line_count <> expected.expected_row_count
   or abs(fact.fact_revenue - expected.expected_revenue) > 0.01
   or fact.fact_units <> expected.expected_units
