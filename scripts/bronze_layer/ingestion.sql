-- Bronze Layer Schema and Table Definitions
-- ERP/CRM Data Warehouse

-- Create bronze schema if not exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- CRM Tables
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
    cst_id integer,
    cst_key varchar(40),
    cst_firstname varchar(40),
    cst_lastname varchar(40),
    cst_marital_status varchar(40),
    cst_gndr varchar(40),
    cst_create_date varchar(40)
);

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
    prd_id integer,
    prd_key varchar(40),
    prd_nm varchar(40),
    prd_cost integer,
    prd_line varchar(40),
    prd_start_dt varchar(40),
    prd_end_dt varchar(40)
);

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
    sls_ord_num varchar(40),
    sls_prd_key varchar(40),
    sls_cust_id varchar(40),
    sls_order_dt varchar(40),
    sls_ship_dt varchar(40),
    sls_due_dt varchar(40),
    sls_sales varchar(40),
    sls_quantity varchar(40),
    sls_price integer
);

-- ERP Tables
CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
    cid varchar(40),
    bdate varchar(40),
    gen varchar(40)
);

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid varchar(40),
    cntry varchar(40)
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id varchar(40),
    cat varchar(40),
    subcat varchar(40),
    maintenance varchar(40)
);
