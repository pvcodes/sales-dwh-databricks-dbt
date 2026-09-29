{{
    config(
        materialized="view"
    )
}}

-- Source: CRM customer master. One row per customer, deduplicated on the CRM
-- surrogate key and typed into the warehouse's canonical domain.

with source as (

    select * from {{ source('bronze', 'crm_cust_info') }}

),

renamed as (

    select
        cst_id                                              as customer_id_raw,
        cst_key                                             as customer_key_raw,
        cst_firstname                                       as first_name_raw,
        cst_lastname                                        as last_name_raw,
        cst_marital_status                                  as marital_status_raw,
        cst_gndr                                            as gender_raw,
        cst_create_date                                     as created_date_raw,
        _ingested_at                                        as bronze_ingested_at,
        _source_file                                        as bronze_source_file,
        _batch_id                                           as bronze_batch_id
    from source

),

typed as (

    select
        try_cast(nullif(trim(customer_id_raw), '') as integer)          as customer_id,
        nullif(upper(trim(customer_key_raw)), '')                       as customer_key,
        nullif(trim(first_name_raw), '')                                as customer_first_name,
        nullif(trim(last_name_raw), '')                                 as customer_last_name,
        nullif(upper(trim(marital_status_raw)), '')                     as marital_status_code,
        nullif(upper(trim(gender_raw)), '')                             as gender_code,
        try_cast(nullif(trim(created_date_raw), '') as date)            as customer_created_date,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from renamed

),

deduplicated as (

    -- The CRM export is a change feed, not a snapshot: some customers appear
    -- more than once with progressively fuller records (a bare key on day 1,
    -- name and gender on day 3). `cst_create_date` is the version timestamp.
    --
    -- Records are ranked by: newest version first, then by how much of the
    -- record is actually populated, then by ingestion recency. Every term can
    -- tie, so a deterministic winner is always produced - a non-deterministic
    -- pick would make the gold marts churn between runs.
    select
        *,
        row_number() over (
            partition by customer_id
            order by
                coalesce(customer_created_date, date '1900-01-01') desc,
                (
                    case when customer_first_name is not null then 8 else 0 end
                  + case when customer_last_name  is not null then 4 else 0 end
                  + case when gender_code         is not null then 2 else 0 end
                  + case when marital_status_code is not null then 1 else 0 end
                ) desc,
                bronze_ingested_at desc,
                bronze_batch_id desc
        ) as _customer_id_recency
    from typed
    where customer_id is not null

),

final as (

    select
        customer_id,
        customer_key,
        customer_first_name,
        customer_last_name,
        concat_ws(' ', customer_first_name, customer_last_name)         as customer_name,
        case
            when marital_status_code = 'M'         then 'Married'
            when marital_status_code = 'S'         then 'Single'
            when marital_status_code = 'MARRIED'   then 'Married'
            when marital_status_code = 'SINGLE'    then 'Single'
            else 'Unknown'
        end                                                             as marital_status,
        case
            when gender_code in ('M', 'MALE')      then 'Male'
            when gender_code in ('F', 'FEMALE')    then 'Female'
            else 'Unknown'
        end                                                             as gender,
        customer_created_date,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from deduplicated
    where _customer_id_recency = 1

)

select * from final
