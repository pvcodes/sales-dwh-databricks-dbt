# Runbook

## Daily operations

Everything is orchestrated by the Asset Bundle; this is what runs unattended.

| Schedule (UTC) | Job | What it does |
|---|---|---|
| 02:30 | `sales_dwh_daily` | Ingestion → `dbt build` |
| 04:00 | `sales_dwh_bronze_maintenance` | `OPTIMIZE` + `VACUUM` on bronze |
| Mon 05:00 | `sales_dwh_weekly_rebuild` | `dbt build --full-refresh` |

Run one-off:

```bash
databricks bundle run sales_dwh_daily -t dev
```

## The two expected warnings

Every build ends with these. They are source defects, not pipeline defects, and
they are set to `severity: warn` on purpose so the count stays visible:

```
WARN 4  source_not_null_bronze_crm_cust_info_cst_id
WARN 6  dbt_utils_source_unique_combination_of_columns_bronze_crm_cust_info_cst_id
```

A healthy build is **`PASS=146 WARN=2 ERROR=0`**. If either count changes, the
upstream extract changed shape — investigate before accepting.

## Local workflow

```bash
make install      # once
make test         # bronze load + full build + incremental build + freshness
make rebuild      # full refresh after changing a filter
make incremental  # exercise the merge path only
make serve        # http://localhost:8080
```

`make clean` removes `.local/`, `dbt/target/` and `dbt/logs/`.

## Triage

### `ERROR ... assert_fact_reconciles_to_sales`

The fact no longer matches silver. Almost always a **stale row from a merge**:
a row that silver no longer produces but the fact still holds.

```bash
make rebuild
```

If it persists after a full refresh, the incremental predicate is wrong — check
`fact_lookback_days` in `dbt_project.yml` against the date range you changed.

### `ERROR ... assert_sales_rejection_rate`

Silver is dropping more than 1% of source rows. A clean extract rejects ~0.07%
(43 of 60,398), so anything above 1% means the source format changed.

```sql
select * from dbt_test_failures.assert_sales_rejection_rate;
```

Look at which filter is rejecting, then decide whether the rule or the parser
needs updating. Do **not** simply raise the threshold to make it green.

### `ERROR ... assert_product_version_integrity`

The current-version tiebreak stopped producing exactly one row per product key.
Usually a source change in `prd_start_dt` or a duplicate `product_version_id`.
The failure query names the offending keys.

### `ERROR ... assert_fact_referential_integrity`

A fact row has a `customer_sk` or `product_sk` with no dimension match. Check
whether a new product appeared without a current version:

```sql
select distinct product_key
from gold.fct_sales
where product_sk not in (select product_sk from gold.dim_products);
```

### `ERROR freshness of bronze.*`

Nothing has landed within 72 hours. Check the ingestion job, then the landing
zone and the Auto Loader checkpoint. Note that `_ingested_at` is set by the
loader, not by the source, so a successful-but-empty run still refreshes
freshness — cross-check row counts in `bronze.ingest_audit`.

### Category name looks stale in the analytics marts

Expected. Descriptive attributes denormalised onto the incremental fact are only
refreshed inside the lookback window, so a renamed category leaves historical
rows on the old label.

```bash
make rebuild              # local
databricks bundle run sales_dwh_weekly_rebuild -t dev   # Databricks
```

### Rescued rows in a bronze table

Auto Loader could not parse a file. The rows are in `_rescued_data` rather than
dropped, and ingestion does not fail on them.

```sql
select _source_file, count(*)
from bronze.crm_sales_details
where _rescued_data is not null
group by 1;
```

Usually a header rename or a stray delimiter in one file. Fix the extract, drop
the bad file, re-run ingestion. If the schema genuinely changed, extend the
`SourceSpec.schema` in `jobs/bronze_auto_loader.py` — `addNewColumns` handles
additive change automatically.

## Changing a filter

The one procedure that reliably causes trouble. A merge cannot delete rows, so
tightening a filter leaves the previously-accepted rows behind.

1. Change the model.
2. `make test` locally — this includes a full refresh, so it will catch the
   fan-out and the stale rows.
3. On Databricks, run `sales_dwh_weekly_rebuild`, or `dbt build --full-refresh`
   for a one-off.

## Adding a source column

1. Add the field to the `SourceSpec.schema` in
   `jobs/bronze_auto_loader.py` (and `ingest/load_bronze_local.py`).
2. Reference it in the staging model.
3. `make test`.

`on_schema_change: sync_all_columns` on the fact handles additive changes in the
warehouse, but the model still has to surface the column.

## Ad-hoc queries

```bash
cd dbt
uv run dbt show --profiles-dir profiles --inline \
  "select category_name, round(gross_revenue) from gold.agg_products__category_rollup order by 2 desc"
```

## First Databricks deploy

Nothing has been run on Databricks yet, so treat this as a smoke test:

1. Set `landing_root`, `checkpoint_root`, `schema_root` as bundle variables.
2. Export `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`.
3. Create the `sales_dwh_dev` catalog and `bronze` schema.
4. `databricks bundle validate && databricks bundle deploy -t dev`
5. `databricks bundle run sales_dwh_daily -t dev`
6. Confirm the two expected warnings and `ERROR=0`.

Known differences to watch on the first Spark run: date-function dialect is
covered by the macros and was verified by render, but `stg_crm__products` regex
handling and Delta `mergeSchema` behaviour are the two places where a DuckDB
success is least predictive of a Spark success.
