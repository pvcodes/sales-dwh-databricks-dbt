#!/usr/bin/env python3
"""ERP/CRM Data Warehouse - Bronze Layer ETL Pipeline."""

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# print(PROJECT_ROOT)

from config import connect
from bronze_layer import run_bronze_layer

DATASETS = {
    "crm_cust_info": {
        "tablename": "crm_cust_info",
        "schema": "bronze",
        "filename": "datasets/source_crm/cust_info.csv",
        "description": "Customer information from CRM"
    },
    "crm_prd_info": {
        "tablename": "crm_prd_info",
        "schema": "bronze",
        "filename": "datasets/source_crm/prd_info.csv",
        "description": "Product information from CRM"
    },
    "crm_sales_details": {
        "tablename": "crm_sales_details",
        "schema": "bronze",
        "filename": "datasets/source_crm/sales_details.csv",
        "description": "Sales transaction details from CRM"
    },
    "erp_cust_az12": {
        "tablename": "erp_cust_az12",
        "schema": "bronze",
        "filename": "datasets/source_erp/CUST_AZ12.csv",
        "description": "Customer data from ERP"
    },
    "erp_loc_a101": {
        "tablename": "erp_loc_a101",
        "schema": "bronze",
        "filename": "datasets/source_erp/LOC_A101.csv",
        "description": "Location data from ERP"
    },
    "erp_px_cat_g1v2": {
        "tablename": "erp_px_cat_g1v2",
        "schema": "bronze",
        "filename": "datasets/source_erp/PX_CAT_G1V2.csv",
        "description": "Product category data from ERP"
    },
}


def list_tables():
    """List all available tables for ingestion."""
    print("Available tables for bronze layer ingestion:")
    print("-" * 50)
    for name, info in DATASETS.items():
        print(f"  {name}: {info['description']}")
    print("-" * 50)


def ingest_tables(conn, tables=None, data_dir=None):
    """Ingest specified tables or all tables if none specified."""
    if tables is None:
        tables = list(DATASETS.keys())

    results = []
    for table_name in tables:
        if table_name not in DATASETS:
            print(f"Warning: Unknown table '{table_name}', skipping...")
            results.append({
                "table": table_name,
                "error": "Unknown table",
                "status": "skipped"
            })
            continue

        dataset = DATASETS[table_name]
        if data_dir:
            filepath = os.path.join(data_dir,
                                    os.path.basename(dataset["filename"]))
        else:
            filepath = PROJECT_ROOT / dataset["filename"]

        print(f"\nIngesting {dataset['tablename']}...")
        try:
            rowcount = run_bronze_layer(conn,
                                        tablename=dataset["tablename"],
                                        schema=dataset["schema"],
                                        filename=str(filepath))
            results.append({
                "table": table_name,
                "rows": rowcount,
                "status": "success"
            })
        except Exception as e:
            results.append({
                "table": table_name,
                "error": str(e),
                "status": "failed"
            })
            print(f"Failed to ingest {table_name}: {e}")

    return results


def print_summary(results):
    """Print ingestion summary."""
    print("\n" + "=" * 60)
    print("Ingestion Summary")
    print("=" * 60)
    for result in results:
        if result["status"] == "success":
            print(f"  {result['table']}: {result['rows']} rows loaded")
        elif result["status"] == "skipped":
            print(
                f"  {result['table']}: SKIPPED - {result.get('error', 'Unknown')}"
            )
        else:
            print(
                f"  {result['table']}: FAILED - {result.get('error', 'Unknown error')}"
            )

    success_count = sum(1 for r in results if r["status"] == "success")
    total = len(results)
    print(f"\nCompleted: {success_count}/{total} tables loaded successfully")


def create_bronze_layer_tables(conn):
    """Create bronze layer tables."""
    try:
        # pass
        cursor = conn.cursor()
        ddl_file_path = os.path.join(PROJECT_ROOT,
                                     "sql_scripts/bronze_layer/ddl.sql")
        print('Tables are being created/recreated in bronze layer.')
        cursor.execute(open(ddl_file_path, 'r').read())
    except Exception as e:
        raise Exception("Failed to create bronze layer tables\n", e)


def main():
    """Main entry point for the ERP/CRM Data Warehouse ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="ERP/CRM Data Warehouse - Bronze Layer ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                     # Load all tables
  python main.py -t crm_cust_info    # Load specific table
  python main.py -t crm_cust_info crm_prd_info  # Load multiple tables
  python main.py --list              # List available tables
  python main.py --data-dir /path/to/data  # Use custom data directory
        """)
    parser.add_argument("-t",
                        "--tables",
                        nargs="+",
                        help="Specific tables to ingest (default: all tables)")
    parser.add_argument("--list",
                        action="store_true",
                        help="List available tables and exit")
    parser.add_argument("--data-dir",
                        help="Custom directory containing CSV data files")
    parser.add_argument("--config", help="Path to database configuration file")

    args = parser.parse_args()

    if args.list:
        list_tables()
        return 0

    print("=" * 60)
    print("ERP/CRM Data Warehouse - Bronze Layer Ingestion")
    print("=" * 60)

    conn = None
    try:
        conn = connect()
        create_bronze_layer_tables(conn)
        results = ingest_tables(conn,
                                tables=args.tables,
                                data_dir=args.data_dir)
        print_summary(results)

        success_count = sum(1 for r in results if r["status"] == "success")
        return 0 if success_count == len(results) else 1

    except Exception as e:
        print(f"Pipeline failed: {e}")
        return 1
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    sys.exit(main())
