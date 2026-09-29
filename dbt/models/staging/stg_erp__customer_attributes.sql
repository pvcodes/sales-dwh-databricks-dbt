{{
    config(
        materialized="view"
    )
}}

-- Source: ERP customer attributes (AZ12).
--
-- `CID` arrives as `NASAW00011000`; stripping only the leading `NAS` system
-- prefix yields the CRM natural key (`AW00011000`) that joins to
-- `stg_crm__customers.customer_key`.

with source as (

    select * from {{ source('bronze', 'erp_cust_az12') }}

),

renamed as (

    select
        cid                    as customer_id_raw,
        bdate                  as birth_date_raw,
        gen                    as gender_raw,
        _ingested_at           as bronze_ingested_at,
        _source_file           as bronze_source_file,
        _batch_id              as bronze_batch_id
    from source

),

typed as (

    select
        nullif(upper(trim(customer_id_raw)), '')                          as erp_customer_id,
        nullif(upper(regexp_replace(trim(customer_id_raw), '^NAS', '')), '') as customer_key,
        try_cast(nullif(trim(birth_date_raw), '') as date)                as birth_date,
        nullif(upper(trim(gender_raw)), '')                               as gender_code,
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
                coalesce(birth_date, date '1900-01-01') desc,
                (case when gender_code is not null then 1 else 0 end) desc,
                bronze_ingested_at desc,
                bronze_batch_id desc
        ) as _recency
    from typed
    where erp_customer_id is not null
      and customer_key is not null

)

select
    erp_customer_id,
    customer_key,
    birth_date,
    case
        when gender_code in ('M', 'MALE')     then 'Male'
        when gender_code in ('F', 'FEMALE')   then 'Female'
        else 'Unknown'
    end                                                             as gender,
    bronze_ingested_at,
    bronze_source_file,
    bronze_batch_id
from deduplicated
where _recency = 1
