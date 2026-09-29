-- Product lifecycle integrity for the type 2 dimension.
--
-- Two properties are checked, both of which would silently corrupt historical
-- reporting if broken:
--   * each product key has exactly one current version, so "current product
--     attributes" is unambiguous
--   * version windows do not overlap, so a point-in-time lookup cannot match
--     two different prices for the same product on the same day

with per_key as (

    select
        product_key,
        count(*)                                          as version_count,
        sum(case when is_current then 1 else 0 end)       as current_count
    from {{ ref('dim_products') }}
    group by 1

),

windowed as (

    select
        product_key,
        valid_from_date,
        valid_to_date,
        lead(valid_from_date) over (
            partition by product_key order by valid_from_date
        ) as next_valid_from_date
    from {{ ref('dim_products') }}

),

violations as (

    select
        'missing_current_version' as failure_reason,
        coalesce(sum(case when current_count <> 1 then 1 else 0 end), 0) as offending_keys
    from per_key

    union all

    select
        'overlapping_version_window',
        coalesce(sum(
            case when next_valid_from_date is not null
                  and valid_to_date >= next_valid_from_date
                 then 1 else 0 end
        ), 0)
    from windowed

    union all

    select
        'inverted_version_window',
        coalesce(sum(
            case when valid_to_date < valid_from_date then 1 else 0 end
        ), 0)
    from windowed

)

select *
from violations
where offending_keys > 0
