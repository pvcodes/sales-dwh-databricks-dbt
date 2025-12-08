-- CRM TABLES
COPY bronze.crm_cust_info 
FROM '/dataset/source_crm/cust_info.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ','
)

COPY bronze.crm_prd_info 
FROM '/dataset/source_crm/prd_info.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ','
)

COPY bronze.crm_sales_details 
FROM '/dataset/source_crm/sales_details.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ','
)


-- ERP TABLES
COPY bronze.erp_cust_az12 
FROM '/dataset/source_erp/cust_az12.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ','
)

COPY bronze.erp_loc_a101 
FROM '/dataset/source_erp/loc_a101.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ','
)

COPY bronze.erp_px_cat_g1v2 
FROM '/dataset/source_erp/px_cat_g1v2.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ','
)