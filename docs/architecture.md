# Architecture

## Layer contracts

Each layer has one job, and the contract is what makes the layers independent.

| Layer | Schema | Materialisation | Contract |
|---|---|---|---|
| bronze | `bronze` | Delta, append-only | Raw strings. No casting, no filtering, no dedup. Plus lineage metadata. |
| silver | `silver` | Views | Typed, trimmed, deduped, cross-system conformed. Rejects invalid rows and records why. |
| gold | `gold` | Tables (fact incremental) | Star schema. Surrogate keys, SCD2 history, pre-joined descriptive attributes. |
| analytics | `gold` | Tables | Pre-aggregated marts. No dependencies on each other. |

Bronze holds everything as VARCHAR on purpose. Parsing, typing and validation
are dbt concerns, so a rule change is a `dbt build` rather than a re-ingest.
The cost is that nothing is queryable in bronze without the staging layer, which
is the right trade for a layer whose only job is to be replayable.

## Model DAG

```
bronze (6 tables)
  │
  ├─ stg_crm__customers ──────────┐
  ├─ stg_erp__customer_attributes ├──▶ int_customers__conformed ──▶ dim_customers ──┐
  ├─ stg_erp__customer_geography ─┘                                                       │
  │                                                                                     ▼
  ├─ stg_crm__products ──▶ int_products__current_version ──▶ dim_products ──▶ fct_sales ──▶ 4 marts
  │                                    ▲                        (SCD2)          ▲
  ├─ stg_erp__product_categories ─────┘                                       │
  │                                                                            │
  └─ stg_crm__sales_details ──────────────────────────────────▶ int_sales__enriched ┘
                                                                   │
                                                              dim_dates
```

## Design decisions

### Deduplicate before joining, and make the choice deterministic

`prd_info` is a change feed: 397 rows, 295 product keys, 77 keys with multiple
versions. Joining `sales_details` straight to it duplicates every sales line
that matches a multi-version product, inflating the fact from 60,398 to 89,818
rows — a 49% error that still passes a naive `not_null` test on every column.

`int_products__current_version` collapses to one row per key with an explicit
tiebreak chain:

1. `is_current_version` (the source marks the open-ended row)
2. latest `product_start_date`
3. highest `product_version_id`

Every step is a total order, so the result is reproducible across runs and
engines. `assert_product_version_integrity` fails the build if that stops
holding, and `dim_products` keeps the full SCD2 history for anyone who needs to
reconstruct a past state.

### Normalise keys, then verify they were worth normalising

Three systems spell the same customer three ways:

| Source | Raw | Normalised |
|---|---|---|
| `crm_cust_info.cst_key` | `AW00011000` | `AW00011000` |
| `erp_cust_az12.CID` | `NASAW00011000` | `AW00011000` |
| `erp_loc_a101.CID` | `AW-00011000` | `AW00011000` |

The same pattern appears in products: `prd_key` is category-prefixed, and the
sales key is its 6th character onward (`BI-RB-BK-R64Y-48` → `R64Y-48`). Both
were verified to join 1:1 *before* the normalisation logic was written, because
a key rule that silently drops rows is worse than one that visibly fails.

### Reject invalid rows, and police the rejection rate

43 of 60,398 sales rows are dropped: 25 with non-positive or unparseable
measures, 19 with compact dates that do not parse, 1 in both categories.
Filtering them in silver keeps the fact clean and makes the loss auditable.

Silent filtering is the real risk, so `assert_sales_rejection_rate` fails the
build if the rate exceeds 1%. A broken extract surfaces as a failed job rather
than a quietly smaller fact table.

### Merge cannot delete, so plan for a rebuild

`fct_sales` merges on `sales_line_id`. That is correct and fast for appends and
corrections, but a merge has no notion of deletion: when the date filter was
tightened, 18 rows already in the fact became permanently orphaned and a plain
`dbt build` could not remove them.

Options considered:

| Approach | Verdict |
|---|---|
| `merge` + scheduled full refresh | **Chosen.** Simple model, one weekly job, guaranteed convergence. |
| `replaceWhere` on a date partition | Fast, but only correct for date-scoped invalidation and not for key corrections. |
| Soft-delete flag + filtered views | Correct but pervasive: every model needs the flag. |

The cost is a weekly rebuild and up to 7 days of staleness for invalidated rows.
That is acceptable for an analytics warehouse and is documented rather than
hidden.

### Keep dialects out of the models

No model contains adapter-specific SQL. Everything goes through
`dbt/macros/cross_db.sql` (date parsing, date arithmetic, date parts) and
`dbt/macros/databricks_config.sql` (Delta format, Liquid Clustering, which
returns an empty config on DuckDB).

This is what makes the DuckDB target possible, and the DuckDB target is what
makes the project reviewable: the full graph, all 146 tests and the data catalog
run on a laptop with no credentials. It also caught real portability bugs —
`extract(week from d)` works on DuckDB but is `week(d)` on Spark.

Intermediate models are **views**, not ephemerals. Ephemeral inlining duplicated
CTE names (`customers`, `joined`) and broke the more complex models. Views cost
a little query-planning time and remove an entire class of failure.

### Keep the two ingestion paths in step

`ingest/load_bronze_local.py` and `jobs/bronze_auto_loader.py` declare the same
six extracts and the same lineage columns, so local and production agree on
schema. The Auto Loader job enforces an explicit schema per extract and parks
unparseable rows in `_rescued_data` — a changed header becomes a quarantined
column rather than a silently shifted one.

## Layering rules

- Staging is 1:1 with a source. No joins. Renaming and casting only.
- Intermediate holds the cross-source logic: dedup, key normalisation, joins.
- Gold is a star schema. The fact carries no logic that belongs in a dimension.
- Analytics marts depend only on gold, and not on each other, so any one can be
  rebuilt or replaced independently.

## Testing strategy

| Layer | How it is tested |
|---|---|
| bronze | Source freshness (24h warn / 72h error) and quarantined-row counts |
| silver | Source defects surfaced as `severity: warn`; rejection rate capped |
| gold | Reconciliation to silver, referential integrity, SCD2 validity, date-order expressions |
| marts | Accepted-value lists, unique combination tests, non-null grain keys |

Source-defect tests are deliberately set to `warn`. A hard failure would mean the
pipeline can never go green on this dataset, at which point people stop reading
the output and the signal is lost. Two warnings that always mean the same thing
are more useful than a red build everyone ignores.
