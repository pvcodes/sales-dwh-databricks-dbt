"""Local bronze ingestion.

Mirrors the semantics of the Databricks Auto Loader job in
``jobs/bronze_auto_loader.py`` so the full medallion pipeline can be built and
tested without a workspace:

* every column lands as ``VARCHAR`` (schema-on-read, no coercion, no cleaning)
* the raw file name, source system, ingestion timestamp and batch id are
  attached as metadata columns
* loads are idempotent per batch and every run is recorded in
  ``bronze.ingest_audit``

Run with::

    uv run python -m ingest.load_bronze_local
    uv run python -m ingest.load_bronze_local --source crm   # single system
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE = PROJECT_ROOT / ".local" / "sales_dwh.duckdb"
DATASETS_DIR = PROJECT_ROOT / "datasets"
BRONZE_SCHEMA = "bronze"

METADATA_COLUMNS = ("_ingested_at", "_source_file", "_source_system", "_batch_id")


@dataclass(frozen=True)
class SourceSpec:
    """Declarative description of one raw extract."""

    table: str
    source_system: str
    relative_path: str
    # Columns that must be present in the header. A mismatch is treated the way
    # Auto Loader treats a schema-change conflict: the file is quarantined.
    required_columns: tuple[str, ...]


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        table="crm_cust_info",
        source_system="crm",
        relative_path="source_crm/cust_info.csv",
        required_columns=(
            "cst_id",
            "cst_key",
            "cst_firstname",
            "cst_lastname",
            "cst_marital_status",
            "cst_gndr",
            "cst_create_date",
        ),
    ),
    SourceSpec(
        table="crm_prd_info",
        source_system="crm",
        relative_path="source_crm/prd_info.csv",
        required_columns=(
            "prd_id",
            "prd_key",
            "prd_nm",
            "prd_cost",
            "prd_line",
            "prd_start_dt",
            "prd_end_dt",
        ),
    ),
    SourceSpec(
        table="crm_sales_details",
        source_system="crm",
        relative_path="source_crm/sales_details.csv",
        required_columns=(
            "sls_ord_num",
            "sls_prd_key",
            "sls_cust_id",
            "sls_order_dt",
            "sls_ship_dt",
            "sls_due_dt",
            "sls_sales",
            "sls_quantity",
            "sls_price",
        ),
    ),
    SourceSpec(
        table="erp_cust_az12",
        source_system="erp",
        relative_path="source_erp/CUST_AZ12.csv",
        required_columns=("CID", "BDATE", "GEN"),
    ),
    SourceSpec(
        table="erp_loc_a101",
        source_system="erp",
        relative_path="source_erp/LOC_A101.csv",
        required_columns=("CID", "CNTRY"),
    ),
    SourceSpec(
        table="erp_px_cat_g1v2",
        source_system="erp",
        relative_path="source_erp/PX_CAT_G1V2.csv",
        required_columns=("ID", "CAT", "SUBCAT", "MAINTENANCE"),
    ),
)


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return handle.readline().rstrip("\r\n").split(",")


def _validate_header(spec: SourceSpec, path: Path) -> None:
    header = [column.strip() for column in _read_header(path)]
    missing = [column for column in spec.required_columns if column not in header]
    if missing:
        raise ValueError(
            f"{spec.table}: file {path.name} is missing required column(s) {missing}. "
            "Treat as a schema evolution conflict and reconcile before re-ingesting."
        )


def _quarantine(con: duckdb.DuckDBPyConnection, table: str, reason: str, path: Path) -> None:
    con.execute(
        f"""
        create table if not exists {BRONZE_SCHEMA}.ingest_quarantine (
            _quarantined_at timestamp,
            _source_table varchar,
            _source_file varchar,
            _reason varchar
        )
        """
    )
    con.execute(
        f"""
        insert into {BRONZE_SCHEMA}.ingest_quarantine
        values (current_timestamp, ?, ?, ?)
        """,
        [table, path.name, reason],
    )


def load_source(
    con: duckdb.DuckDBPyConnection,
    spec: SourceSpec,
    batch_id: str,
    ingested_at: dt.datetime,
    mode: str,
) -> int:
    path = DATASETS_DIR / spec.relative_path
    if not path.exists():
        raise FileNotFoundError(f"missing source extract: {path}")

    _validate_header(spec, path)

    con.execute(f"create schema if not exists {BRONZE_SCHEMA}")
    target = f"{BRONZE_SCHEMA}.{spec.table}"

    read_sql = f"""
        select
            *,
            ? as _ingested_at,
            ? as _source_file,
            ? as _source_system,
            ? as _batch_id
        from read_csv(
            ?,
            header = true,
            all_varchar = true,
            ignore_errors = false
        )
    """

    if mode == "overwrite":
        con.execute(f"create or replace table {target} as {read_sql}", [
            ingested_at, path.name, spec.source_system, batch_id, str(path),
        ])
    else:
        # Append mode: guard against replaying the same file into the same batch.
        con.execute(f"create table if not exists {target} as select * from {read_sql} where false", [
            ingested_at, path.name, spec.source_system, batch_id, str(path),
        ])
        con.execute(
            f"""
            insert into {target}
            select * from {read_sql}
            where not exists (
                select 1 from {target} t where t._batch_id = ?
            )
            """,
            [ingested_at, path.name, spec.source_system, batch_id, str(path), batch_id],
        )

    row_count = con.execute(f"select count(*) from {target}").fetchone()[0]
    return row_count


def run(
    database: Path = DEFAULT_DATABASE,
    source_system: str | None = None,
    mode: str = "overwrite",
) -> None:
    specs = [s for s in SOURCE_SPECS if source_system in (None, s.source_system)]
    if not specs:
        raise ValueError(f"no sources registered for system {source_system!r}")

    database.parent.mkdir(parents=True, exist_ok=True)
    batch_id = uuid.uuid4().hex[:12]
    ingested_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    con = duckdb.connect(str(database))
    try:
        con.execute(f"create schema if not exists {BRONZE_SCHEMA}")
        con.execute(
            f"""
            create table if not exists {BRONZE_SCHEMA}.ingest_audit (
                _batch_id varchar,
                _source_table varchar,
                _source_system varchar,
                _source_file varchar,
                _row_count bigint,
                _load_mode varchar,
                _loaded_at timestamp
            )
            """
        )

        print(f"bronze load batch={batch_id} mode={mode} database={database}")
        for spec in specs:
            try:
                row_count = load_source(con, spec, batch_id, ingested_at, mode)
            except Exception as exc:  # noqa: BLE001 - surfaced into quarantine
                _quarantine(con, spec.table, str(exc), DATASETS_DIR / spec.relative_path)
                print(f"  QUARANTINED {spec.table}: {exc}")
                continue

            con.execute(
                f"""
                insert into {BRONZE_SCHEMA}.ingest_audit values (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    batch_id,
                    spec.table,
                    spec.source_system,
                    os.path.basename(spec.relative_path),
                    row_count,
                    mode,
                    ingested_at,
                ],
            )
            print(f"  {spec.table:<20} {row_count:>7,} rows")

        totals = con.execute(
            f"""
            select _source_system, sum(_row_count) as rows_loaded
            from {BRONZE_SCHEMA}.ingest_audit
            where _batch_id = ?
            group by 1 order by 1
            """,
            [batch_id],
        ).fetchall()
        for system, rows in totals:
            print(f"  total[{system}] = {rows:,}")
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw extracts into the bronze layer.")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--source", choices=["crm", "erp"], default=None)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    args = parser.parse_args()
    run(database=args.database, source_system=args.source, mode=args.mode)


if __name__ == "__main__":
    main()
