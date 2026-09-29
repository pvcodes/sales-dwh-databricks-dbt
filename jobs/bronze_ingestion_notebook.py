"""Notebook entry point for the bronze Auto Loader job.

The bundle passes the landing, checkpoint and schema roots as base parameters,
which arrive as notebook widgets. All of the ingestion logic lives in
``jobs/bronze_auto_loader.py`` so it can also be unit tested and reused outside
Databricks.
"""

import sys
from pathlib import Path

# Bundle notebooks run from the deployment directory, so make the repository's
# job package importable regardless of the working directory.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.bronze_auto_loader import run  # noqa: E402

try:  # pragma: no cover - only meaningful on Databricks
    dbutils.widgets.text("catalog", "sales_dwh_dev", "Unity Catalog catalog")
    dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
    dbutils.widgets.text("landing_root", "", "Object-store landing root")
    dbutils.widgets.text("checkpoint_root", "", "Auto Loader checkpoint root")
    dbutils.widgets.text("schema_root", "", "Auto Loader schema root")
    dbutils.widgets.dropdown("source", "all", ["all", "crm", "erp"], "Source system")
    dbutils.widgets.dropdown("trigger", "batch", ["batch", "stream"], "Read mode")
except Exception:  # noqa: BLE001
    pass

source_filter = None
try:  # noqa: BLE001
    widget = dbutils.widgets.get("source")
    if widget and widget != "all":
        source_filter = widget
except Exception:  # noqa: BLE001
    pass

results = run(
    catalog=dbutils.widgets.get("catalog") if "dbutils" in dir() else "sales_dwh_dev",
    bronze_schema=dbutils.widgets.get("bronze_schema") if "dbutils" in dir() else "bronze",
    landing_root=dbutils.widgets.get("landing_root"),
    checkpoint_root=dbutils.widgets.get("checkpoint_root"),
    schema_root=dbutils.widgets.get("schema_root"),
    source_system=source_filter,
    trigger=dbutils.widgets.get("trigger") if "dbutils" in dir() else "batch",
)

# A non-zero exit on rescued rows would mask legitimate partial-load warnings, so
# ingestion only fails when a table came back empty.
failed = [result for result in results if result["row_count"] == 0]
if failed:
    dbutils.notebook.exit(
        f"ERROR: no rows landed for: {', '.join(r['table'] for r in failed)}"
    )
else:
    total = sum(r["row_count"] for r in results)
    rescued = sum(r["rescued_row_count"] for r in results)
    dbutils.notebook.exit(f"OK: {total:,} rows across {len(results)} tables, {rescued} rescued")
