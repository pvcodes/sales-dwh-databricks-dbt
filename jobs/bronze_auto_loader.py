"""Bronze ingestion with Databricks Auto Loader.

Cloud Storage / ADLS extracts are picked up incrementally, schema evolution is
handled by Auto Loader, and rows that cannot be parsed are diverted to a
rescued-data column rather than failing the job. Everything lands in Delta as
raw strings, which is what makes bronze replayable: the cleansing rules live in
dbt and can be changed without re-ingesting.

The same six extracts are loaded by ``ingest/load_bronze_local.py`` for local
development, so the two paths stay in step.

Exposed as a notebook wrapper (``jobs/bronze_ingestion_notebook.py``) for the
Asset Bundle, or runnable directly:

    python -m jobs.bronze_auto_loader --catalog sales_dwh_dev \\
        --landing-root abfss://landing@<sa>.dfs.core.windows.net/sales_dwh \\
        --checkpoint-root abfss://checkpoints@<sa>.dfs.core.windows.net/sales_dwh \\
        --schema-root abfss://schemas@<sa>.dfs.core.windows.net/sales_dwh
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from typing import Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

try:  # pragma: no cover - only present on a Databricks cluster
    from pyspark.dbutils import DBUtils  # type: ignore

    _dbutils = DBUtils()
    dbutils = _dbutils
except ImportError:  # pragma: no cover - local or non-Databricks runtime
    dbutils = None  # type: ignore[assignment]

RESCUED_COLUMN = "_rescued_data"
INGEST_COLUMNS = (
    "_ingested_at",
    "_source_file",
    "_source_system",
    "_batch_id",
    RESCUED_COLUMN,
)


@dataclass(frozen=True)
class SourceSpec:
    """One raw extract: its landing pattern and its enforced schema."""

    table: str
    source_system: str
    # Glob relative to the landing root, resolved against the job parameter.
    file_pattern: str
    # Enforcing the schema rather than inferring it is what turns a changed
    # header into a quarantined column instead of a silently shifted column.
    schema: StructType


def _strings(*names: str) -> StructType:
    """Build an all-VARCHAR schema; the raw extracts are ingested untyped."""
    return StructType([StructField(name, StringType(), True) for name in names])


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        table="crm_cust_info",
        source_system="crm",
        file_pattern="source_crm/cust_info.csv",
        schema=_strings(
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
        file_pattern="source_crm/prd_info.csv",
        schema=_strings(
            "prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"
        ),
    ),
    SourceSpec(
        table="crm_sales_details",
        source_system="crm",
        file_pattern="source_crm/sales_details.csv",
        schema=_strings(
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
        file_pattern="source_erp/CUST_AZ12.csv",
        schema=_strings("CID", "BDATE", "GEN"),
    ),
    SourceSpec(
        table="erp_loc_a101",
        source_system="erp",
        file_pattern="source_erp/LOC_A101.csv",
        schema=_strings("CID", "CNTRY"),
    ),
    SourceSpec(
        table="erp_px_cat_g1v2",
        source_system="erp",
        file_pattern="source_erp/PX_CAT_G1V2.csv",
        schema=_strings("ID", "CAT", "SUBCAT", "MAINTENANCE"),
    ),
)

SOURCE_TABLES = tuple(spec.table for spec in SOURCE_SPECS)


def get_spark(app_name: str = "sales-dwh-bronze-ingestion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )


def _source_path(landing_root: str, spec: SourceSpec) -> str:
    return f"{landing_root.rstrip('/')}/{spec.file_pattern}"


def _auto_loader_options(spec: SourceSpec, checkpoint_root: str, schema_root: str) -> dict:
    """Options shared by the batch and stream readers."""
    return {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaLocation": f"{schema_root.rstrip('/')}/{spec.table}",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.rescuedDataColumn": RESCUED_COLUMN,
        "cloudFiles.includeFileMetadata": "true",
        # Types are enforced from the declared schema, so a value that changes
        # shape becomes a rescued row instead of a null column.
        "cloudFiles.inferColumnTypes": "false",
        "cloudFiles.schema": spec.schema.json(),
        "checkpointLocation": f"{checkpoint_root.rstrip('/')}/{spec.table}",
    }


def read_batch(
    spark: SparkSession,
    spec: SourceSpec,
    landing_root: str,
    checkpoint_root: str,
    schema_root: str,
    starting_version: int = 1,
) -> DataFrame:
    """One incremental Auto Loader pass, suitable for a scheduled job.

    ``latestVersion`` makes the pass resumable: a rerun picks up where the
    checkpoint left off rather than re-reading the whole landing zone.
    """
    reader = spark.read.format("cloudFiles")
    for key, value in _auto_loader_options(spec, checkpoint_root, schema_root).items():
        reader = reader.option(key, value)
    return (
        reader.option("latestVersion", starting_version)
        .option("maxFilesPerTrigger", 8)
        .load(_source_path(landing_root, spec))
    )


def stream_batches(
    spark: SparkSession,
    spec: SourceSpec,
    landing_root: str,
    checkpoint_root: str,
    schema_root: str,
    starting_version: int = 1,
) -> Iterator[DataFrame]:
    """True streaming reader, for the long-lived Auto Loader deployment."""
    reader = spark.readStream.format("cloudFiles")
    for key, value in _auto_loader_options(spec, checkpoint_root, schema_root).items():
        reader = reader.option(key, value)
    return (
        reader.option("latestVersion", starting_version)
        .option("maxFilesPerTrigger", 4)
        .load(_source_path(landing_root, spec))
    )


def decorate(df: DataFrame, spec: SourceSpec, batch_id: str) -> DataFrame:
    """Attach the lineage columns that dbt's sources and tests rely on."""
    if "_metadata" in df.columns:
        source_file = F.col("_metadata.file_path")
    else:
        # Batch Auto Loader exposes the origin through input_file_name().
        source_file = F.input_file_name()

    decorated = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", source_file)
        .withColumn("_source_system", F.lit(spec.source_system))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # The rescued column is only present when something needed rescuing; keep
    # the column stable so downstream tests can always reference it.
    if RESCUED_COLUMN not in decorated.columns:
        decorated = decorated.withColumn(RESCUED_COLUMN, F.lit(None).cast("string"))

    if "_metadata" in decorated.columns:
        decorated = decorated.drop("_metadata")

    return decorated


def ingest_table(
    spark: SparkSession,
    spec: SourceSpec,
    catalog: str,
    bronze_schema: str,
    landing_root: str,
    checkpoint_root: str,
    schema_root: str,
    batch_id: str,
    trigger: str = "batch",
    starting_version: int = 1,
) -> dict:
    """Ingest one extract into its bronze Delta table.

    Append-only by design: bronze is an immutable landing zone, and the
    deduplication rules live in dbt so they can be revised without re-ingesting.
    """
    target = f"{catalog}.{bronze_schema}.{spec.table}"

    if trigger == "stream":
        reader = stream_batches(
            spark, spec, landing_root, checkpoint_root, schema_root, starting_version
        )
        query = (
            reader.select("*")
            .withColumn("_batch_id", F.lit(batch_id))
            .writeStream.format("delta")
            .option(
                "checkpointLocation", f"{checkpoint_root.rstrip('/')}/stream/{spec.table}"
            )
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()
    else:
        reader = read_batch(
            spark, spec, landing_root, checkpoint_root, schema_root, starting_version
        )
        (
            decorate(reader, spec, batch_id)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target)
        )

    return summarise(spark, target, spec)


def summarise(spark: SparkSession, target: str, spec: SourceSpec) -> dict:
    """Row counts plus the rescued-row count that the bronze tests assert on."""
    row = spark.sql(
        f"""
        select
            count(*)                                          as row_count,
            count_if({RESCUED_COLUMN} is not null)             as rescued_row_count,
            count(distinct _batch_id)                         as batch_count,
            count(distinct _source_file)                      as file_count,
            min(_ingested_at)                                 as first_ingested_at,
            max(_ingested_at)                                 as latest_ingested_at
        from {target}
        """
    ).first()

    return {
        "table": target,
        "source_system": spec.source_system,
        "row_count": int(row["row_count"]),
        "rescued_row_count": int(row["rescued_row_count"] or 0),
        "batch_count": int(row["batch_count"]),
        "file_count": int(row["file_count"]),
        "latest_ingested_at": str(row["latest_ingested_at"]),
    }


def write_audit(spark: SparkSession, catalog: str, bronze_schema: str, batch_id: str, results: list) -> None:
    audit_table = f"{catalog}.{bronze_schema}.ingest_audit"
    spark.sql(
        f"""
        create table if not exists {audit_table} (
            _batch_id            string,
            _source_table        string,
            _source_system       string,
            _row_count           bigint,
            _rescued_row_count   bigint,
            _file_count          bigint,
            _loaded_at           timestamp
        ) using delta
        """
    )
    for result in results:
        spark.sql(
            f"""
            insert into {audit_table} values (
                '{batch_id}',
                '{result["table"].split(".")[-1]}',
                '{result["source_system"]}',
                {result["row_count"]},
                {result["rescued_row_count"]},
                {result["file_count"]},
                current_timestamp()
            )
            """
        )


def run(
    catalog: str,
    bronze_schema: str = "bronze",
    landing_root: str | None = None,
    checkpoint_root: str | None = None,
    schema_root: str | None = None,
    source_system: str | None = None,
    trigger: str = "batch",
    starting_version: int = 1,
) -> list[dict]:
    """Ingest every configured extract, or one source system if filtered."""
    # Fall back to notebook widgets so the same code works as a bundle task.
    if dbutils is not None:
        widgets = {widget.name: widget.get() for widget in dbutils.widgets.all().values()}
        landing_root = landing_root or widgets.get("landing_root")
        checkpoint_root = checkpoint_root or widgets.get("checkpoint_root")
        schema_root = schema_root or widgets.get("schema_root")
        catalog = widgets.get("catalog") or catalog
        bronze_schema = widgets.get("bronze_schema") or bronze_schema

    missing = [
        name
        for name, value in (
            ("landing_root", landing_root),
            ("checkpoint_root", checkpoint_root),
            ("schema_root", schema_root),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"missing required configuration: {', '.join(missing)}")

    spark = get_spark()
    batch_id = uuid.uuid4().hex[:12]
    print(f"bronze ingestion batch={batch_id} catalog={catalog} trigger={trigger}")

    results = []
    for spec in SOURCE_SPECS:
        if source_system and spec.source_system != source_system:
            continue
        result = ingest_table(
            spark=spark,
            spec=spec,
            catalog=catalog,
            bronze_schema=bronze_schema,
            landing_root=landing_root,
            checkpoint_root=checkpoint_root,
            schema_root=schema_root,
            batch_id=batch_id,
            trigger=trigger,
            starting_version=starting_version,
        )
        results.append(result)
        print(
            f"  {spec.table:<20} rows={result['row_count']:>8,} "
            f"rescued={result['rescued_row_count']:>4,} "
            f"files={result['file_count']:>4,} batches={result['batch_count']}"
        )

    write_audit(spark, catalog, bronze_schema, batch_id, results)
    print(json.dumps(results, indent=2, default=str))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto Loader bronze ingestion.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--landing-root")
    parser.add_argument("--checkpoint-root")
    parser.add_argument("--schema-root")
    parser.add_argument("--source", choices=["crm", "erp"], default=None)
    parser.add_argument("--trigger", choices=["batch", "stream"], default="batch")
    parser.add_argument("--starting-version", type=int, default=1)
    args = parser.parse_args()

    run(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        landing_root=args.landing_root,
        checkpoint_root=args.checkpoint_root,
        schema_root=args.schema_root,
        source_system=args.source,
        trigger=args.trigger,
        starting_version=args.starting_version,
    )


if __name__ == "__main__":
    main()
