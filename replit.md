# ERP/CRM Data Warehouse

## Overview
A Python-based data warehouse project implementing a Medallion Architecture (Bronze, Silver, Gold layers) for consolidating ERP and CRM sales data. The system uses PostgreSQL as the database backend.

## Current State
- Bronze layer ingestion is fully operational
- 6 tables loaded into the bronze schema from CSV source files
- ETL pipeline successfully tested and working

## Project Structure
```
erp-crm-data-warehouse/
├── config/                    # Configuration module
│   ├── __init__.py           # Exports load_config, connect
│   ├── main.py               # Database config loading and connection
│   └── database.ini          # PostgreSQL connection settings
├── datasets/                  # Source data files
│   ├── source_crm/           # CRM source data (3 CSV files)
│   └── source_erp/           # ERP source data (3 CSV files)
├── py_scripts/               # Python ETL scripts
│   └── bronze_layer.py       # Bronze layer ingestion logic
├── scripts/                  # SQL scripts
│   └── bronze_layer/         # Bronze layer SQL (schema creation)
├── main.py                   # Main entry point for ETL pipeline
├── pyproject.toml           # Python project configuration
└── uv.lock                  # UV package lock file
```

## How to Run

### Run all tables
```bash
python main.py
```

### Run specific tables
```bash
python main.py -t crm_cust_info crm_prd_info
```

### List available tables
```bash
python main.py --list
```

### Use custom data directory
```bash
python main.py --data-dir /path/to/data
```

## CLI Options
```
-h, --help                    Show help message
-t, --tables TABLE [TABLE...] Specific tables to ingest (default: all)
--list                        List available tables and exit
--data-dir DIR                Custom directory containing CSV files
--config FILE                 Path to database configuration file
```

## Database Schema
### Bronze Layer Tables
| Table | Description | Rows |
|-------|-------------|------|
| bronze.crm_cust_info | Customer information from CRM | 18,494 |
| bronze.crm_prd_info | Product information from CRM | 397 |
| bronze.crm_sales_details | Sales transaction details from CRM | 60,398 |
| bronze.erp_cust_az12 | Customer data from ERP | 18,484 |
| bronze.erp_loc_a101 | Location data from ERP | 18,484 |
| bronze.erp_px_cat_g1v2 | Product category data from ERP | 37 |

## Dependencies
- Python 3.12+
- psycopg2-binary (PostgreSQL adapter)

## Configuration
Database connection is configured in `config/database.ini`:
- Uses Replit's built-in PostgreSQL database
- Connection parameters: host, database, user, password

## Recent Changes
- 2024-12-08: Initial Replit environment setup
  - Fixed cross-platform path handling in config/main.py
  - Configured PostgreSQL database with bronze schema
  - Created all 6 bronze layer tables
  - Updated main.py with argparse-based CLI
  - Added support for selective table ingestion
  - Updated ingestion.sql with complete schema for all tables
  - Successfully tested data ingestion for all source files
