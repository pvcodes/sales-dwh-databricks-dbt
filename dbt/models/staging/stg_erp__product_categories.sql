{{
    config(
        materialized="view"
    )
}}

-- Source: ERP product category reference (PX_CAT_G1V2).
--
-- This is the small dimension that gives the product hierarchy meaning:
-- category -> subcategory, plus whether the SKU needs recurring maintenance.

with source as (

    select * from {{ source('bronze', 'erp_px_cat_g1v2') }}

),

renamed as (

    select
        id                      as category_id_raw,
        cat                     as category_name_raw,
        subcat                  as subcategory_name_raw,
        maintenance             as maintenance_raw,
        _ingested_at            as bronze_ingested_at,
        _source_file            as bronze_source_file,
        _batch_id               as bronze_batch_id
    from source

),

typed as (

    select
        nullif(upper(trim(category_id_raw)), '')            as category_id,
        -- Names are kept in the casing the source uses: they are presentation
        -- labels, and the source already carries them properly cased.
        nullif(trim(regexp_replace(category_name_raw, '\s+', ' ')), '')    as category_name,
        nullif(trim(regexp_replace(subcategory_name_raw, '\s+', ' ')), '') as subcategory_name,
        nullif(upper(trim(maintenance_raw)), '')            as maintenance_code,
        bronze_ingested_at,
        bronze_source_file,
        bronze_batch_id
    from renamed

)

select
    category_id,
    category_name,
    subcategory_name,
    lower(coalesce(maintenance_code, 'no'))                as maintenance_code,
    case maintenance_code
        when 'Y'  then true
        when 'YES' then true
        when 'N'  then false
        when 'NO' then false
        else false
    end                                                    as requires_maintenance,
    category_id || ' | ' || category_name                 as category_label
from typed
where category_id is not null
