-- No raw ISO country code may survive into the conformed geography.
--
-- The ERP extract mixes `USA`/`US`/`DE` with full English names. Anything still
-- matching a two-letter code means a new code was added to the feed without the
-- mapping in `stg_erp__customer_geography` being extended, which would split a
-- single country across two dimension values in every report.

select
    geo.country as unmapped_country,
    count(*)    as customer_count
from {{ ref('stg_erp__customer_geography') }} as geo
where geo.country <> 'Unknown'
  and length(geo.country) = 2
  and upper(geo.country) = geo.country
group by 1
