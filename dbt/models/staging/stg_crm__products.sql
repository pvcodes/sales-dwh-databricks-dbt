{{
    config(
        materialized="view"
    )
}}

-- Source: CRM product master. One row per product *version*.
--
-- Two source quirks are resolved here:
--   1. `prd_line` arrives space padded inside quotes (`"R "`).
--   2. `prd_end_dt` is blank for live products, and for discontinued products it
--      holds the date the *previous* version ended, i.e. it is earlier than
--      `prd_start_dt`. Such rows are treated as open ended.

with source as (

    select * from {{ source('bronze', 'crm_prd_info') }}

),

renamed as (

    select
        prd_id                                              as product_version_id_raw,
        prd_key                                             as product_key_qualified_raw,
        prd_nm                                              as product_name_raw,
        prd_cost                                            as product_cost_raw,
        prd_line                                            as product_line_raw,
        prd_start_dt                                        as start_date_raw,
        prd_end_dt                                          as end_date_raw,
        _ingested_at                                        as bronze_ingested_at,
        _source_file                                        as bronze_source_file,
        _batch_id                                           as bronze_batch_id
    from source

),

typed as (

    select
        try_cast(nullif(trim(product_version_id_raw), '') as integer)      as product_version_id,
        nullif(upper(trim(product_key_qualified_raw)), '')                  as product_key_qualified,
        nullif(trim(product_name_raw), '')                                 as product_name_raw,
        try_cast(nullif(trim(product_cost_raw), '') as decimal(12, 2))     as product_cost,
        nullif(upper(trim(product_line_raw)), '')                          as product_line_code,
        try_cast(nullif(trim(start_date_raw), '') as date)                 as start_date,
        try_cast(nullif(trim(end_date_raw), '') as date)                   as end_date,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from renamed

),

key_parsed as (

    select
        *,
        -- `BI-RB-BK-R64Y-48` -> category token `BI-RB`, sales key `BK-R64Y-48`.
        substr(product_key_qualified, 1, 5)                                 as category_key_prefix,
        replace(substr(product_key_qualified, 1, 5), '-', '_')             as category_id,
        nullif(substr(product_key_qualified, 7), '')                        as product_key
    from typed
    where product_key_qualified is not null
      and length(product_key_qualified) > 6

),

normalised as (

    select
        product_version_id,
        product_key_qualified,
        product_key,
        category_id,
        category_key_prefix,
        -- "HL Road Frame - Black- 58" -> "HL Road Frame Black 58"
        trim(regexp_replace(regexp_replace(product_name_raw, '-', ' '), '\s+', ' '))  as product_name,
        product_cost,
        case
            when product_line_code = 'R' then 'Road'
            when product_line_code = 'T' then 'Touring'
            when product_line_code = 'S' then 'Standard'
            when product_line_code = 'M' then 'Mountain'
            else 'Other'
        end                                                                 as product_line,
        start_date,
        case
            when end_date is null or end_date < start_date then null
            else end_date
        end                                                                 as end_date,
        -- Explicit open-ended sentinel so "is this version current?" is a
        -- single, non-nullable comparison downstream.
        case
            when end_date is null or end_date < start_date then date '9999-12-31'
            else end_date
        end                                                                 as effective_end_date,
        case
            when end_date is null or end_date < start_date then true
            else false
        end                                                                 as is_current_version,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from key_parsed

)

select * from normalised
