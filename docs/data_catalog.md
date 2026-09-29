# Data catalog

Column reference for the whole warehouse. Generated from the DuckDB build;
regenerate with `make docs` for the browsable version.

## bronze

All columns are `VARCHAR` — raw strings, no casting, no filtering. Every table
carries the same lineage columns:

| Column | Type | Meaning |
|---|---|---|
| `_ingested_at` | `TIMESTAMP` | When the row landed. Source of `dbt source freshness`. |
| `_source_file` | `VARCHAR` | Origin file, for tracing a bad row back to an extract. |
| `_source_system` | `VARCHAR` | `crm` or `erp`. |
| `_batch_id` | `VARCHAR` | Ingestion run identifier. |
| `_rescued_data` | `VARCHAR` | Auto Loader only. Unparseable content, quarantined rather than dropped. |

| Table | Rows | Source columns |
|---|---|---|
| `crm_cust_info` | 18,494 | `cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, `cst_marital_status`, `cst_gndr`, `cst_create_date` |
| `crm_prd_info` | 397 | `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt` |
| `crm_sales_details` | 60,398 | `sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price` |
| `erp_cust_az12` | 18,484 | `CID`, `BDATE`, `GEN` |
| `erp_loc_a101` | 18,484 | `CID`, `CNTRY` |
| `erp_px_cat_g1v2` | 37 | `ID`, `CAT`, `SUBCAT`, `MAINTENANCE` |

## silver

### `stg_crm__customers` → 18,484 rows

Change feed deduplicated to one row per `cst_id`, ranked by `customer_created_date`
descending. Of 18,494 raw rows, 4 have a blank `cst_id` (dropped) and 6 are
duplicates from the change feed.

Normalises `M`/`S` → `Married`/`Single` and `M`/`F`/`MALE`/`FEMALE` → `Male`/`Female`;
unknown values become `Unknown` rather than null. Adds a `completeness_score`
so downstream can rank attribute-rich customers.

### `stg_erp__customer_attributes` / `stg_erp__customer_geography` → 18,484 rows each

Normalise `CID`: strip the leading `NAS` (`NASAW00011000` → `AW00011000`) and
remove dashes (`AW-00011000` → `AW00011000`). `BDATE` is ISO `yyyy-mm-dd`.
`CNTRY` mixes ISO codes, country names and blanks, so the code/name mapping is
flagged rather than assumed.

### `stg_erp__product_categories` → 37 rows

`category_id` and `subcategory_name` preserved as the source presents them
(`Bikes`, `Road Bikes`). 7 products use a `CO-PE` prefix with no matching
category row.

### `stg_crm__products` → 397 rows, `int_products__current_version` → 295 rows

Splits the qualified `prd_key` into a category prefix (`BI-RB-BK`) and a
`category_id` (`BI_RB_BK`). Derives `is_current_version` and an open-ended
`effective_end_date` (`9999-12-31`). The intermediate view applies the
deterministic current-version selection; see
[architecture.md](architecture.md#deduplicate-before-joining-and-make-the-choice-deterministic).

| Column | Type | Notes |
|---|---|---|
| `product_key` | `VARCHAR` | Sales key, `substr(prd_key, 7)` |
| `product_version_id` | `INTEGER` | Tiebreak of last resort |
| `product_line` | `VARCHAR` | `Road` / `Touring` / `Standard` / `Mountain` |
| `category_id` | `VARCHAR` | `CO_RF`, `BI_RB_BK`, … |
| `is_current_version` | `BOOLEAN` | One per key, guaranteed |
| `product_lifecycle_days` | `BIGINT` | Days from start to effective end |

### `stg_crm__sales_details` → 60,355 rows

Parses compact `yyyyMMdd` dates, filters non-positive measures and unparseable
dates, and requires `order_date <= ship_date <= due_date`. Derives
`days_to_ship`, `days_to_due`, `days_in_transit`, `discount_amount` and
`realised_unit_price`.

Rejected: **25** rows with bad measures, **19** with unparseable dates, **1**
counted in both — 43 total.

### `int_customers__conformed` → 18,484 rows

Joins CRM to both ERP customer extracts on the normalised key. Columns:
`customer_id`, `customer_key`, `customer_first_name`, `customer_last_name`,
`customer_name`, `marital_status`, `gender`, `birth_date`, `country`,
`customer_created_date`, `current_age`, `age_in_days`, `age_band`.

### `int_sales__enriched` → 60,355 rows

Sales joined to the current product version and the conformed customer. This is
the reconciliation anchor for `fct_sales` — row count, revenue and units must
match exactly.

## gold — dimensions

### `dim_customers` — 18,484 rows, 24 columns

Type 1. Surrogate key `md5('cust|' || customer_key)`. Carries CRM attributes,
ERP birth date and geography, plus order rollup: `order_line_count`,
`order_count`, `total_units`, `lifetime_sales_amount`, `lifetime_gross_margin`,
`first_order_date`, `last_order_date`, `days_since_first_order`,
`days_since_last_order`, `is_customer`. **18,482 of 18,484 customers have
orders**; the 2 without are legitimate and flagged via `is_customer`.

### `dim_products` — 397 rows, 20 columns

Type 2 SCD2. 295 keys, exactly one current row each, non-overlapping intervals.

| Column | Notes |
|---|---|
| `product_sk` | Surrogate key including the version, so it is unique across history |
| `valid_from_date` / `valid_to_date` | Interval, inclusive; current rows run to `9999-12-31` |
| `is_current` | Exactly one `TRUE` per `product_key` |
| `version_number` / `version_count` | Sequence and total versions per key |
| `has_product_cost` | `FALSE` for the 2 products with no cost |

### `dim_dates` — 2,191 rows, 19 columns

`2010-01-01` → `2015-12-31` (bounds are `reporting_start_date` /
`reporting_end_date` in `dbt_project.yml`; the end bound is exclusive).
`date_key` is a `DATE`. Adds `date_year_quarter_key` / `date_year_month_key`
for sort-safe grouping, plus `days_ago`, `is_past`, `is_today`, `is_weekend`.

## gold — `fct_sales`

Grain: **one row per sales line**. 60,355 rows, 27,656 distinct orders,
60,364 units, $29,347,844 revenue.

Incremental: `merge` on `sales_line_id`, 3-day lookback
(`fact_lookback_days`), `on_schema_change: sync_all_columns`.

| Group | Columns |
|---|---|
| Keys | `sales_line_id`, `order_number`, `customer_sk`, `customer_id`, `customer_key`, `product_sk`, `product_key`, `product_version_id` |
| Dates | `order_date`, `ship_date`, `due_date`, `days_to_ship`, `days_to_due`, `days_in_transit` |
| Date attributes | `order_year`, `order_quarter`, `order_month_number`, `order_month_name`, `order_month_start`, `order_day_of_year`, `order_day_of_week`, `order_week_of_year`, `order_day_of_month`, `order_day_name`, `order_year_quarter`, `order_year_month` |
| Measures | `sales_amount`, `quantity`, `unit_price`, `discount_amount`, `realised_unit_price`, `product_cost`, `gross_margin`, `gross_margin_pct`, `unit_margin_pct` |
| Denormalised product | `product_name`, `product_line`, `product_version_count`, `category_id`, `category_name`, `subcategory_name`, `requires_maintenance` |
| Denormalised customer | `customer_name`, `customer_gender`, `customer_marital_status`, `customer_age_band`, `customer_country` |
| Lineage | `bronze_ingested_at` |

The natural keys (`customer_key`, `product_key`, `product_version_id`) are
stored alongside the surrogate keys so the fact is self-describing when queried
without the dimensions — and so a dimension that fails to join is visible as a
mismatch rather than a null.

> Denormalised attributes are refreshed only within the lookback window. If a
> category is renamed, historical fact rows keep the old label until the weekly
> full refresh. This is the cost of incremental materialisation, documented in
> the [runbook](runbook.md).

## gold — analytics marts

| Model | Rows | Grain | Contents |
|---|---|---|---|
| `agg_kpi__monthly_performance` | 38 | month | Orders, active customers, units, revenue, margin, margin %, AOV, MoM and YoY growth |
| `agg_products__performance` | 130 | product | Revenue, margin, ABC class, cumulative revenue share, price/margin tiers |
| `agg_products__category_rollup` | 22 | category × subcategory × line × maintenance | Distinct orders/buyers/products, units, revenue, revenue and margin share |
| `agg_customers__rfm_segmentation` | 18,482 | buyer | Recency/frequency/monetary scores and segment |

`agg_products__performance` covers the 130 of 295 products that have sales; the
inner join is intentional. `agg_customers__rfm_segmentation` covers buyers only.

Distinct counts in `agg_products__category_rollup` are computed at the category
grain in a separate CTE. Rolling up a per-product aggregate would double-count
an order that spans two products in the same category.

ABC distribution: `A` 34 products ($23.4M), `B` 36 ($4.4M), `C` 60 ($1.5M) —
the 34 A-class products carry 80% of revenue.

RFM distribution: `At Risk` 3,832, `Hibernating` 3,562, `Loyal Customers` 3,299,
`Need Attention` 2,943, `Recent Customers` 2,390, `Champions` 2,112,
`Big Spenders` 344.

> **Recency does not discriminate on this dataset.** Sales end 2014-01-28, so
> measured against the current date every buyer has ~4,600–5,700 days of
> recency. The `ntile` split is technically correct but the buckets are
> effectively identical, and segment membership is driven by frequency and
> monetary value rather than recency. Treat the recency dimension as noise here,
> or re-anchor `current_date_expr()` to the dataset's last order date if recency
> becomes the point of the analysis.

## Configurable variables

Set in `dbt/dbt_project.yml`:

| Var | Default | Purpose |
|---|---|---|
| `reporting_start_date` | `2010-01-01` | `dim_dates` start, inclusive |
| `reporting_end_date` | `2016-01-01` | `dim_dates` end, **exclusive** |
| `fact_lookback_days` | `3` | Incremental re-processing window |
| `max_sales_rejection_rate` | `0.01` | Fail the build if silver drops more than 1% of source rows |
| `bronze_schema` / `silver_schema` / `gold_schema` | `bronze` / `silver` / `gold` | Schema names |
| `bronze_vacuum_retention_days` | `30` | Bronze history retention |
