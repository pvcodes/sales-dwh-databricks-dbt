# Session Context

**Current Task**
Completed the medallion sales DWH (Auto Loader → dbt bronze/silver/gold → analytics) and added interview-prep study guides.

**Key Decisions**
- `fct_sales` uses `merge` plus a scheduled weekly `--full-refresh`; merge cannot delete, which left 18 orphaned rows when a date filter tightened.
- Source-defect tests (`cst_id` blanks/duplicates) are `severity: warn`; the 1% sales rejection-rate test stays a hard failure.
- No adapter-specific SQL in models — all dialect differences route through `dbt/macros/cross_db.sql` so the DuckDB target can run the full graph.

**Next Steps**
- Databricks execution is unverified (no workspace); first deploy is a smoke test — check product-model regex and Delta `mergeSchema` first.
- Add mart-level reconciliation: KPI and category marts should tie to `fct_sales` as a singular test.
- Commit is pending; nothing staged, 53 new/modified paths including `dbt/`, `databricks.yml`, `docs/`, `Makefile`, CI.

**Notes**
- Healthy build is `PASS=146 WARN=2 ERROR=0`. Run `make test` to reproduce.
- Repo uses local DuckDB (`dbt/` cwd or `SALES_DWH_DUCKDB_PATH`); legacy PostgreSQL files in `py_scripts/`, `scripts/`, `config/` are retained untouched.
