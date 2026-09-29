# Sales DWH — CRM + ERP warehouse on Databricks

A medallion data warehouse that lands raw CRM and ERP extracts with **Auto
Loader**, conforms them into a star schema with **dbt** on **Delta Lake**, and
publishes aggregate marts for reporting.

The project runs end-to-end on your laptop against DuckDB with no warehouse and
no cloud account, and runs unchanged on Databricks against Unity Catalog.

```
raw extracts ──▶ bronze ──▶ silver ──▶ gold ──▶ analytics
  (CSV/ADLS)     raw         conformed    star      aggregates
                strings      typed,       schema    RFM, ABC,
                            deduped,               category
                            validated             rollups
```

## Quick start

```bash
make install     # Python + dbt dependencies
make test        # load bronze, build everything, run every test
```

That runs the real pipeline against the sample extracts in `datasets/`. To
browse the result:

```bash
make serve       # dbt catalog on http://localhost:8080
```

`make help` lists every target. Everything is also plain `dbt` and `python`
commands — the Makefile is a convenience, not a dependency.

## What gets built

| Layer | Schema | Contents |
|---|---|---|
| bronze | `bronze` | 6 raw extracts, all columns VARCHAR, plus lineage metadata |
| silver | `silver` | 6 staging views + 3 intermediate views (dedup, conform, enrich) |
| gold | `gold` | `dim_customers`, `dim_products` (SCD2), `dim_dates`, `fct_sales`, 4 aggregate marts |

Against the sample data the pipeline produces:

| Model | Rows | Notes |
|---|---|---|
| `bronze.crm_sales_details` | 60,398 | raw |
| `silver.int_sales__enriched` | 60,355 | 43 rows rejected (25 bad measures, 19 unparseable dates, 1 in both) |
| `gold.fct_sales` | 60,355 | reconciles exactly to silver |
| `gold.dim_customers` | 18,484 | 18,494 raw rows deduplicated by change-feed recency |
| `gold.dim_products` | 397 | 295 SCD2 versions, exactly one current per key |
| `gold.dim_dates` | 2,191 | 2010-01-01 → 2015-12-31 |

Fact totals: **27,656 orders**, **60,364 units**, **$29,347,844 revenue**,
**$11,681,753 gross margin**, spanning 2010-12-29 → 2014-01-28, with **0 orphan
fact rows**.

## The problems this actually solves

**A change feed is not a dimension.** `crm_cust_info` contains 18,494 rows for
18,484 customers, and `crm_prd_info` holds 397 rows for 295 products. Joining
sales to products without deduplicating first fans 60,398 rows out to 89,818 —
a 49% inflation of the fact table. `stg_crm__products` and
`int_products__current_version` apply a deterministic ranking
(`is_current`, then latest `start_date`, then highest `product_version_id`) so
the version choice is reproducible rather than arbitrary. `assert_product_version_integrity`
fails the build if the choice stops being unique.

**Two systems spell the same customer three different ways.** CRM uses
`AW00011000`; ERP `CUST_AZ12` uses `NASAW00011000`; ERP `LOC_A101` uses
`AW-00011000`. `int_customers__conformed` normalises all three before joining.
The keys were verified to match 1:1 before the normalisation was written.

**Bad data should be dropped, not silently propagated.** 25 sales rows carry
non-positive or unparseable measures and 19 carry compact dates that do not
parse. Both are filtered in silver, and `assert_sales_rejection_rate` fails the
build if the rejection rate ever exceeds 1% — so a broken extract is caught
rather than quietly shrinking the fact table.

**Row counts alone prove nothing.** `assert_fact_reconciles_to_sales` compares
row count, revenue and units between silver and gold. It has already earned its
keep: it caught stale rows left behind by a merge after the date filter was
tightened.

## Layout

```
dbt/                      dbt project
  models/staging/         1:1 conformed views per source
  models/intermediate/    dedup, cross-system joins
  models/marts/core/      dimensions + incremental fact
  models/marts/analytics/ aggregate marts
  macros/                 cross-dialect SQL, Delta config
  tests/singular/         cross-model data-quality assertions
ingest/                   local bronze loader (DuckDB)
jobs/                     Databricks Auto Loader job
maintenance/              Delta OPTIMIZE / VACUUM
databricks.yml            Asset Bundle: jobs, clusters, targets
Makefile                  canonical local commands
docs/                     architecture, data catalog, runbook
```

Legacy PostgreSQL scaffolding (`py_scripts/`, `scripts/bronze_layer/`,
`config/`) is retained from the original project and is not part of this
pipeline.

## Running on Databricks

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run sales_dwh_daily -t dev
```

`databricks.yml` defines four jobs:

| Job | Schedule | Purpose |
|---|---|---|
| `sales_dwh_bronze_ingestion` | on demand | Auto Loader → bronze |
| `sales_dwh_daily` | on demand | ingestion → `dbt build` |
| `sales_dwh_weekly_rebuild` | Mondays 05:00 UTC | `dbt build --full-refresh` |
| `sales_dwh_bronze_maintenance` | daily 04:00 UTC | `OPTIMIZE` + `VACUUM` |

Set `landing_root`, `checkpoint_root` and `schema_root` as bundle variables to
your storage account, and provide `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH` and
`DATABRICKS_TOKEN` for the `databricks` dbt target in `dbt/profiles/profiles.yml`.

### Why there is a weekly full refresh

`fct_sales` is incremental with `merge` on `sales_line_id`. Merge can add and
update rows but **cannot delete** them, so when upstream invalidates a row — a
retracted sales line, a corrected product key, a date that stops parsing — the
stale copy survives. This is not hypothetical: tightening the date filter left
18 orphaned rows in the fact until a full refresh was run.

The Monday job is the mechanism that guarantees convergence. Trade-off: any
change to a silver filter needs either a full refresh or a backfill window
covering the affected dates, and the fact will be stale for up to seven days
otherwise. This is a deliberate choice — a delete-aware incremental (via
`replaceWhere` or a soft-delete flag) would remove the weekly rebuild at the
cost of a much more complex model.

## Cross-dialect design

Every adapter-specific expression goes through a macro in
`dbt/macros/cross_db.sql`; models contain no dialect literals. That is what
lets the identical model graph build on DuckDB locally and Spark in production:

| Macro | DuckDB | Spark / Databricks |
|---|---|---|
| `parse_compact_date` | `try_cast(try_strptime(x,'%Y%m%d') as date)` | `try_to_date(x,'yyyyMMdd')` |
| `subtract_days` | `(d - n)` | `date_sub(d, n)` |
| `day_of_month_expr` | `extract(day from d)` | `dayofmonth(d)` |
| `date_part` | `extract(part from d)` | `part(d)` |

`dbt/macros/databricks_config.sql` applies Delta file format and Liquid
Clustering only on Databricks and returns an empty config on DuckDB.

## Testing

146 tests pass on every run. Two are **expected warnings**, not failures:

```
WARN 4  source_not_null_bronze_crm_cust_info_cst_id
WARN 6  dbt_utils_source_unique_combination_of_columns_bronze_crm_cust_info_cst_id
```

These document real defects in the source extract — 4 rows with a blank
`cst_id` and 6 duplicated IDs from the change feed. They are configured at
`severity: warn` rather than suppressed, so the moment the count changes you
see it. A hard failure would mean the pipeline can never go green on this data,
which is worse than useless as a signal.

Beyond the standard dbt tests, five singular tests carry the real logic:
rejection rate, fact/silver reconciliation, fact referential integrity, SCD2
interval validity, and unmapped country codes.

CI (`.github/workflows/ci.yml`) runs the bronze load, `dbt build --full-refresh`,
a second **incremental** `dbt build`, docs generation, source freshness, bundle
validation and a Python syntax check.

## Known limitations

- **Databricks execution is unverified.** The models compile for Spark and the
  bundle is structurally valid, but no workspace was available, so no run has
  happened on Databricks. Treat the first deploy as a smoke test.
- **Incremental deletion** requires the weekly full refresh (above).
- **7 products use a `CO-PE` category prefix** that does not exist in the ERP
  category table. They are kept under an `UNCLASSIFIED` bucket by a left join
  rather than dropped. They currently have no sales.
- **2 products have no cost**, so their `gross_margin` is null. Neither has any
  sales, so the fact is unaffected today, but a future sale against one would
  produce a null margin.
- **Fixed reporting window.** `reporting_start_date` / `reporting_end_date`
  cover 2010-01-01 → 2016-01-01 in `dbt_project.yml`; extend them for newer data.
- **RFM recency is degenerate on this data.** Sales end 2014-01-28, so every
  buyer is ~4,600–5,700 days from their last order and the recency buckets are
  effectively identical. Frequency and monetary value carry the segmentation.
  Re-anchor the recency measure to the dataset's last order date if recency
  needs to discriminate.

## Further reading

- [docs/architecture.md](docs/architecture.md) — layer contracts, model DAG, design decisions
- [docs/data_catalog.md](docs/data_catalog.md) — column-level reference for every model
- [docs/runbook.md](docs/runbook.md) — operational procedures and failure triage
- [docs/interview-prep.md](docs/interview-prep.md) — study guide: pitch, numbers, the five decisions that earn an interview
- [docs/interview-drills.md](docs/interview-drills.md) — timed Q&A drill with model answers
