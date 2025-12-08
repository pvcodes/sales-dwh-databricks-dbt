import sys
from pathlib import Path
# import time

try:
  project_root = Path(__file__).resolve().parent.parent
  sys.path.append(str(project_root))
except Exception as e:
  print("Failed to add project root to sys.path", e)
  # __file__ may not exist in some environments; ignore safely
  pass

from config import connect
from py_scripts.bronze_layer import run_bronze_layer

# import psycopg2
# from psycopg2 import sql

if __name__ == "__main__":
  # Optionally add project root to sys.path
  conn = connect()

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
