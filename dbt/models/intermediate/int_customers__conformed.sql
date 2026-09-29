{{
    config(
        materialized="view"
    )
}}

-- Conformed customer at one row per CRM customer, with the ERP demographic and
-- geographic attributes attached.
--
-- Customers are kept even when they have never ordered: a dimension that only
-- contains active buyers silently misreports the customer base size.

with customers as (

    select * from {{ ref('stg_crm__customers') }}

),

attributes as (

    select * from {{ ref('stg_erp__customer_attributes') }}

),

geography as (

    select * from {{ ref('stg_erp__customer_geography') }}

),

joined as (

    select
        customers.customer_id,
        customers.customer_key,
        customers.customer_first_name,
        customers.customer_last_name,
        customers.customer_name,
        customers.marital_status,
        -- Prefer the CRM value, fall back to the ERP extract, then 'Unknown'.
        coalesce(nullif(customers.gender, 'Unknown'), attributes.gender, 'Unknown')  as gender,
        attributes.birth_date,
        geography.country,
        customers.customer_created_date
    from customers
    left join attributes  on customers.customer_key = attributes.customer_key
    left join geography   on customers.customer_key = geography.customer_key

),

derived as (

    select
        *,
        {{ date_diff_days('birth_date', current_date_expr()) }} as age_in_days,
        {{ full_years_between('birth_date', current_date_expr()) }} as current_age
    from joined

)

select
    customer_id,
    customer_key,
    customer_first_name,
    customer_last_name,
    customer_name,
    marital_status,
    gender,
    birth_date,
    country,
    customer_created_date,
    current_age,
    age_in_days,
    case
        when birth_date is null                              then 'Unknown'
        when current_age < 18                                then 'Under 18'
        when current_age between 18 and 24                   then '18-24'
        when current_age between 25 and 34                   then '25-34'
        when current_age between 35 and 44                   then '35-44'
        when current_age between 45 and 54                   then '45-54'
        when current_age between 55 and 64                   then '55-64'
        when current_age >= 65                               then '65+'
        else 'Unknown'
    end                                                     as age_band
from derived
