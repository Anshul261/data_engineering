-- =============================================================================
-- Bronze Layer: Bulk Load from CSV Sources
-- =============================================================================
-- Uses \COPY (client-side) so no superuser privilege is required.
-- Run with: psql -U <user> -d <database> -f scripts/bronze_load.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- CRM: crm_cust_info
-- -----------------------------------------------------------------------------
TRUNCATE TABLE bronze.crm_cust_info;

\COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
FROM '/home/anshul/projects/data_engineering/data/engineering/source_crm/cust_info.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- -----------------------------------------------------------------------------
-- CRM: crm_prd_info
-- -----------------------------------------------------------------------------
TRUNCATE TABLE bronze.crm_prd_info;

\COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
FROM '/home/anshul/projects/data_engineering/data/engineering/source_crm/prd_info.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- -----------------------------------------------------------------------------
-- CRM: crm_sales_details
-- -----------------------------------------------------------------------------
TRUNCATE TABLE bronze.crm_sales_details;

\COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
FROM '/home/anshul/projects/data_engineering/data/engineering/source_crm/sales_details.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- -----------------------------------------------------------------------------
-- ERP: erp_cust_az12
-- -----------------------------------------------------------------------------
TRUNCATE TABLE bronze.erp_cust_az12;

\COPY bronze.erp_cust_az12 (cid, bdate, gen)
FROM '/home/anshul/projects/data_engineering/data/engineering/source_erp/CUST_AZ12.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- -----------------------------------------------------------------------------
-- ERP: erp_loc_a101
-- -----------------------------------------------------------------------------
TRUNCATE TABLE bronze.erp_loc_a101;

\COPY bronze.erp_loc_a101 (cid, cntry)
FROM '/home/anshul/projects/data_engineering/data/engineering/source_erp/LOC_A101.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- -----------------------------------------------------------------------------
-- ERP: erp_px_cat_g1v2
-- -----------------------------------------------------------------------------
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

\COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
FROM '/home/anshul/projects/data_engineering/data/engineering/source_erp/PX_CAT_G1V2.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');
