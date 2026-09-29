"""Delta maintenance for the bronze layer.

Bronze is append-only and gets a lot of small Auto Loader output, so it needs
both compaction and history pruning. Pruning is safe here specifically because
bronze is replayable: every file still exists in the landing zone, and the
cleansing rules live in dbt rather than in the raw layer.

The silver and gold layers are *not* pruned with the same retention. They are
built by dbt and can be rebuilt from bronze, so they get a short retention too,
but the point is that no job should hold a time travel window longer than it can
actually reconstruct.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.bronze_auto_loader import SOURCE_TABLES, get_spark  # noqa: E402

BRONZE_SCHEMA = "bronze"


def _tables_to_maintain(spark, catalog: str, schema: str) -> list[str]:
    """Resolve the tables in a schema.

    Bronze has a fixed set of extracts, so it is driven from SOURCE_SPECS to
    guarantee every raw table is compacted even if a run produced no new files.
    """
    if schema == BRONZE_SCHEMA:
        return list(SOURCE_TABLES)
    return [row["tableName"] for row in spark.sql(f"show tables in {catalog}.{schema}").collect()]


def optimise_and_vacuum(
    spark,
    catalog: str,
    schemas: dict[str, int],
    dry_run: bool = False,
) -> list[dict]:
    """OPTIMIZE then VACUUM each table.

    VACUUM is the destructive step, so it is skipped entirely on a dry run and
    only ever runs against a table that was compacted first.
    """
    results = []
    for schema, retention_hours in schemas.items():
        for name in _tables_to_maintain(spark, catalog, schema):
            qualified = f"{catalog}.{schema}.{name}"
            spark.sql(f"optimize {qualified}").collect()
            vacuumed = False
            if not dry_run:
                # retentionDuration is in HOURS, matching the dbt var of the
                # same intent expressed in days.
                spark.sql(f"vacuum {qualified} retain {retention_hours} hours").collect()
                vacuumed = True
            results.append(
                {"table": qualified, "retention_hours": retention_hours, "vacuumed": vacuumed}
            )
            print(f"  optimised {qualified} (retain {retention_hours}h, vacuumed={vacuumed})")
    return results


def main() -> None:
    try:  # pragma: no cover - only meaningful on Databricks
        dbutils.widgets.text("catalog", "sales_dwh_dev", "Unity Catalog catalog")
        dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
        dbutils.widgets.text("retention_hours", "720", "Bronze retention, hours")
        dry_run = dbutils.widgets.get("dry_run", "false").lower() == "true"
    except Exception:  # noqa: BLE001
        dry_run = False

    spark = get_spark("sales-dwh-bronze-maintenance")
    catalog = dbutils.widgets.get("catalog")
    bronze_schema = dbutils.widgets.get("bronze_schema")

    schemas = {
        bronze_schema: int(dbutils.widgets.get("retention_hours", "720")),
    }
    print(f"maintenance catalog={catalog} dry_run={dry_run}")
    results = optimise_and_vacuum(spark, catalog, schemas, dry_run=dry_run)
    dbutils.notebook.exit(f"OK: {len(results)} tables maintained")


if __name__ == "__main__":
    main()
