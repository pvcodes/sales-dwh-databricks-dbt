{{
    config(
        materialized="table",
        tags=["analytics", "customer"]
    )
}}

-- Customer segmentation using an RFM (recency, frequency, monetary) model.
--
-- Each customer is scored 1-5 on each of the three measures with `ntile`, then
-- combined into a named segment that the marketing team selects from. Only
-- customers who have actually purchased are scored; non-buyers stay in
-- `dim_customers` but are not given a misleading segment.
--
-- Grain: one row per customer who has placed at least one order.

with customers as (

    select * from {{ ref('dim_customers') }}

),

buyers as (

    select
        customer_sk,
        customer_id,
        customer_key,
        customer_name,
        gender,
        age_band,
        country
    from customers
    where is_customer

),

activity as (

    select
        buyers.*,
        {{ date_diff_days('max(order_date)', current_date_expr()) }} as recency_days,
        count(*)                                    as frequency_orders,
        sum(sales_amount)                           as monetary_value,
        max(order_date)                             as last_order_date
    from {{ ref('fct_sales') }} as fct
    inner join buyers on fct.customer_sk = buyers.customer_sk
    group by 1, 2, 3, 4, 5, 6, 7

),

scored as (

    select
        *,
        -- 5 is best on all three: lowest recency, highest frequency, highest
        -- value. `ntile` assigns the highest bucket to the *last* row of the
        -- ordering, so recency must be sorted descending to make the smallest
        -- number of days since purchase score highest. Frequency and monetary
        -- already sort ascending.
        ntile(5) over (order by recency_days desc)        as recency_score,
        ntile(5) over (order by frequency_orders)        as frequency_score,
        ntile(5) over (order by monetary_value)           as monetary_score
    from activity

)

select
    customer_sk,
    customer_id,
    customer_key,
    customer_name,
    gender,
    age_band,
    country,
    recency_days,
    frequency_orders,
    monetary_value,
    last_order_date,
    recency_score,
    frequency_score,
    monetary_score,
    recency_score + frequency_score + monetary_score     as rfm_total_score,
    case
        when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4
            then 'Champions'
        when recency_score >= 3 and frequency_score >= 4
            then 'Loyal Customers'
        when recency_score >= 4 and frequency_score <= 2
            then 'Recent Customers'
        -- At Risk: used to buy often, but has not been seen for a long time.
        when recency_score <= 2 and frequency_score >= 3
            then 'At Risk'
        when recency_score <= 2 and frequency_score <= 2
            then 'Hibernating'
        when monetary_score = 5
            then 'Big Spenders'
        else 'Need Attention'
    end                                                  as customer_segment
from scored
