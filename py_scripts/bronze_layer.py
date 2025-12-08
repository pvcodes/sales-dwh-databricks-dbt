import sys
from pathlib import Path
import time

try:
    project_root = Path(__file__).resolve().parent.parent
    sys.path.append(str(project_root))
except Exception as e:
    print("Failed to add project root to sys.path", e)
    # __file__ may not exist in some environments; ignore safely
    pass

import psycopg2
from psycopg2 import sql
from config import load_config


def connect(config):
    """Connect to PostgreSQL and return a live connection."""
    try:
        # Do NOT use a context manager on the connection; it will close on exit.
        conn = psycopg2.connect(**config)
        # Optional: set autocommit False and manage transactions explicitly.
        conn.autocommit = False
        print("Connected to the PostgreSQL server.")
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Database connection error: {error}")
        raise


def run_bronze_layer(conn, tablename, schema, filename):
    """
    Truncate target table and copy the CSV into it using COPY.
    Assumes the CSV has a header row (use CSV HEADER).
    """
    if not conn or not tablename or not schema or not filename:
        raise ValueError(
            "conn, tablename, schema and filename must be provided")

    start = time.time()

    # Build safe SQL identifiers
    tbl_ident = sql.Identifier(schema, tablename)

    try:
        with conn.cursor() as cur:
            # 1) Truncate (fast, requires privileges)
            truncate_stmt = sql.SQL("TRUNCATE TABLE {}").format(tbl_ident)
            cur.execute(truncate_stmt)

            # 2) COPY from file with HEADER; stream file directly
            copy_stmt = sql.SQL(
                "COPY {} FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ',', QUOTE '\"')"
            ).format(tbl_ident)

            # Open the file and stream it
            # Set encoding explicitly to match your CSV
            with open(filename, "r", encoding="utf-8") as f:
                cur.copy_expert(copy_stmt.as_string(cur), f)

            # 3) Optional verification: count rows
            count_stmt = sql.SQL("SELECT COUNT(*) FROM {}").format(tbl_ident)
            cur.execute(count_stmt)
            (rowcount, ) = cur.fetchone()

        # Commit if everything succeeded
        conn.commit()
        elapsed = time.time() - start
        print(
            f"Data copied successfully. {rowcount} rows loaded in {elapsed:.2f}s."
        )

        return rowcount

    except Exception as e:
        # Rollback to leave the database consistent
        conn.rollback()
        print(f"Error during load. Rolled back. Details: {e}")
        raise


if __name__ == "__main__":
    # Optionally add project root to sys.path
    config = load_config()
    conn = connect(config)

    try:
        # Example executions (uncomment as needed)
        # run_bronze_layer(conn,
        #                  tablename="crm_cust_info",
        #                  schema="bronze",
        #                  filename="datasets/source_crm/cust_info.csv")

        # run_bronze_layer(conn,
        #                  tablename="crm_prd_info",
        #                  schema="bronze",
        #                  filename="datasets/source_crm/prd_info.csv")

        run_bronze_layer(conn,
                         tablename="crm_sales_details",
                         schema="bronze",
                         filename="datasets/source_crm/sales_details.csv")
    finally:
        conn.close()
        print("Connection closed.")
