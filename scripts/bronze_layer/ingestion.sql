CREATE DATABASE erp_crm_dw;
CREATE SCHEMA bronze


CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
	cst_id integer,
	cst_key varchar(40),
	cst_firstname varchar(40),
	cst_lastname varchar(40),
	cst_marital_status varchar(40),
	cst_gndr varchar(40),
	cst_create_date varchar(40)
);

COPY bronze.crm_cust_info 
FROM '/erp-crm-data-warehouse/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
	prd_id integer,
	prd_key varchar(40),
	prd_nm varchar(40),
	prd_cost integer,
	prd_line varchar(40),
	prd_start_dt varchar(40),
	prd_end_dt varchar(40)
);


COPY bronze.crm_prd_info 
FROM '/erp-crm-data-warehouse/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;

SELECT * FROM bronze.crm_prd_info LIMIT 20;


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


TRUNCATE TABLE bronze.crm_sales_details;

COPY bronze.crm_sales_details 
FROM '/erp-crm-data-warehouse/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;


SELECT COUNT(*) FROM bronze.crm_sales_details LIMIT 20;