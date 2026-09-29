# Canonical entry points for the project. Every dbt command runs from dbt/ so
# that the relative DuckDB path in profiles.yml resolves against the repo.

DBT        := uv run dbt
DBT_DIR    := dbt
DB         := .local/sales_dwh.duckdb
CI_DB      := .local/sales_dwh_ci.duckdb
PROFILES   := profiles

.DEFAULT_GOAL := help

.PHONY: help install bronze build rebuild incremental test docs serve freshness \
        check ci clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install Python and dbt dependencies
	uv sync --dev
	cd $(DBT_DIR) && $(DBT) deps --profiles-dir $(PROFILES)

bronze: ## Load the sample extracts into the bronze layer
	uv run python -m ingest.load_bronze_local --mode overwrite --database $(DB)

build: ## Run the dbt models and tests incrementally
	cd $(DBT_DIR) && $(DBT) build --profiles-dir $(PROFILES) --no-partial-parse

rebuild: ## Drop and rebuild every model (needed after changing a filter)
	cd $(DBT_DIR) && $(DBT) build --profiles-dir $(PROFILES) --no-partial-parse --full-refresh

incremental: ## Exercise only the incremental fact and its downstream marts
	cd $(DBT_DIR) && $(DBT) build --profiles-dir $(PROFILES) --no-partial-parse --select fct_sales+

test: bronze rebuild incremental ## Full local verification, exactly as CI runs it
	cd $(DBT_DIR) && $(DBT) source freshness --profiles-dir $(PROFILES) --no-partial-parse

docs: ## Generate the dbt data catalog
	cd $(DBT_DIR) && $(DBT) docs generate --profiles-dir $(PROFILES) --no-partial-parse

serve: ## Serve the data catalog locally on http://localhost:8080
	cd $(DBT_DIR) && $(DBT) docs serve --profiles-dir $(PROFILES) --no-partial-parse --port 8080

freshness: ## Check source freshness
	cd $(DBT_DIR) && $(DBT) source freshness --profiles-dir $(PROFILES) --no-partial-parse

ci: ## Run the CI pipeline against an isolated DuckDB file
	uv run python -m ingest.load_bronze_local --mode overwrite --database $(CI_DB)
	cd $(DBT_DIR) && $(DBT) build --profiles-dir $(PROFILES) --target ci --no-partial-parse --full-refresh
	cd $(DBT_DIR) && $(DBT) build --profiles-dir $(PROFILES) --target ci --no-partial-parse --select fct_sales+

clean: ## Remove the local DuckDB databases and dbt build artifacts
	rm -rf .local $(DBT_DIR)/target $(DBT_DIR)/logs
