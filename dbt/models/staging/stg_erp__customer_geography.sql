{{
    config(
        materialized="view"
    )
}}

-- Source: ERP customer geography (A101).
--
-- `CID` arrives dash separated (`AW-00011000`); the dashes are removed to reach
-- the same natural key used by the CRM and AZ12 extracts. `CNTRY` mixes ISO
-- codes with English names, so codes are folded to their canonical name.

with source as (

    select * from {{ source('bronze', 'erp_loc_a101') }}

),

renamed as (

    select
        cid                    as customer_id_raw,
        cntry                  as country_raw,
        _ingested_at           as bronze_ingested_at,
        _source_file           as bronze_source_file,
        _batch_id              as bronze_batch_id
    from source

),

typed as (

    select
        nullif(upper(trim(customer_id_raw)), '')                          as erp_customer_id,
        nullif(upper(replace(trim(customer_id_raw), '-', '')), '')         as customer_key,
        nullif(trim(country_raw), '')                                      as country_raw_clean,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from renamed

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by erp_customer_id
            order by
                (case when country_raw_clean is not null then 1 else 0 end) desc,
                country_raw_clean asc,
                bronze_ingested_at desc,
                bronze_batch_id desc
        ) as _recency
    from typed
    where erp_customer_id is not null
      and customer_key is not null

),

normalised as (

    select
        erp_customer_id,
        customer_key,
        case
            when country_raw_clean is null                       then 'Unknown'
            when upper(country_raw_clean) in ('US', 'USA')       then 'United States'
            when upper(country_raw_clean) in ('UK', 'GB')        then 'United Kingdom'
            when upper(country_raw_clean) in ('DE')              then 'Germany'
            when upper(country_raw_clean) in ('FR')              then 'France'
            when upper(country_raw_clean) in ('CA')              then 'Canada'
            when upper(country_raw_clean) in ('AU')              then 'Australia'
            when upper(country_raw_clean) in ('NL')              then 'Netherlands'
            when upper(country_raw_clean) in ('ES')              then 'Spain'
            when upper(country_raw_clean) in ('SE')              then 'Sweden'
            when upper(country_raw_clean) in ('IT')              then 'Italy'
            when upper(country_raw_clean) in ('CN')              then 'China'
            when upper(country_raw_clean) in ('JP')              then 'Japan'
            when upper(country_raw_clean) in ('IN')              then 'India'
            when upper(country_raw_clean) in ('SG')              then 'Singapore'
            when upper(country_raw_clean) in ('ZA')              then 'South Africa'
            when upper(country_raw_clean) in ('NZ')              then 'New Zealand'
            when upper(country_raw_clean) in ('AR')              then 'Argentina'
            when upper(country_raw_clean) in ('MX')              then 'Mexico'
            when upper(country_raw_clean) in ('SA')              then 'Saudi Arabia'
            when upper(country_raw_clean) in ('EG')              then 'Egypt'
            when upper(country_raw_clean) in ('TR')              then 'Turkey'
            when upper(country_raw_clean) in ('IR')              then 'Iran'
            when upper(country_raw_clean) in ('PK')              then 'Pakistan'
            when upper(country_raw_clean) in ('CO')              then 'Colombia'
            when upper(country_raw_clean) in ('NO')              then 'Norway'
            when upper(country_raw_clean) in ('FI')              then 'Finland'
            when upper(country_raw_clean) in ('DK')              then 'Denmark'
            when upper(country_raw_clean) in ('BE')              then 'Belgium'
            when upper(country_raw_clean) in ('AT')              then 'Austria'
            when upper(country_raw_clean) in ('CH')              then 'Switzerland'
            when upper(country_raw_clean) in ('PL')              then 'Poland'
            when upper(country_raw_clean) in ('PT')              then 'Portugal'
            when upper(country_raw_clean) in ('IE')              then 'Ireland'
            else country_raw_clean
        end                                                                     as country
    from deduplicated
    where _recency = 1

)

select * from normalised
