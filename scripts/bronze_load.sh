#!/bin/bash
set -e  # exit immediately on any error
# =============================================================================
# Bronze Layer: Bulk Load from CSV Sources into Dockerized PostgreSQL
# =============================================================================
# Usage: bash scripts/bronze_load.sh
# Run from project root: /home/anshul/projects/data_engineering
# =============================================================================

CONTAINER="data_warehouse"
DB="data_warehouse"
USER="warehouse_user"
DATA_DIR="/home/anshul/projects/data_engineering/data/engineering"

echo ">>> Creating temp directories inside container..."
docker exec "$CONTAINER" mkdir -p /tmp/source_crm
docker exec "$CONTAINER" mkdir -p /tmp/source_erp

echo ">>> Copying CSV files into container..."
docker cp "$DATA_DIR/source_crm/cust_info.csv"    "$CONTAINER":/tmp/source_crm/cust_info.csv
docker cp "$DATA_DIR/source_crm/prd_info.csv"      "$CONTAINER":/tmp/source_crm/prd_info.csv
docker cp "$DATA_DIR/source_crm/sales_details.csv" "$CONTAINER":/tmp/source_crm/sales_details.csv
docker cp "$DATA_DIR/source_erp/CUST_AZ12.csv"     "$CONTAINER":/tmp/source_erp/CUST_AZ12.csv
docker cp "$DATA_DIR/source_erp/LOC_A101.csv"       "$CONTAINER":/tmp/source_erp/LOC_A101.csv
docker cp "$DATA_DIR/source_erp/PX_CAT_G1V2.csv"   "$CONTAINER":/tmp/source_erp/PX_CAT_G1V2.csv

echo ">>> Loading bronze tables..."

# CRM
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "TRUNCATE TABLE bronze.crm_cust_info;"
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) FROM '/tmp/source_crm/cust_info.csv' WITH (FORMAT CSV, HEADER TRUE, NULL '');"
echo "    crm_cust_info loaded"

docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "TRUNCATE TABLE bronze.crm_prd_info;"
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt) FROM '/tmp/source_crm/prd_info.csv' WITH (FORMAT CSV, HEADER TRUE, NULL '');"
echo "    crm_prd_info loaded"

docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "TRUNCATE TABLE bronze.crm_sales_details;"
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price) FROM '/tmp/source_crm/sales_details.csv' WITH (FORMAT CSV, HEADER TRUE, NULL '');"
echo "    crm_sales_details loaded"

# ERP
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "TRUNCATE TABLE bronze.erp_cust_az12;"
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "COPY bronze.erp_cust_az12 (cid, bdate, gen) FROM '/tmp/source_erp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER TRUE, NULL '');"
echo "    erp_cust_az12 loaded"

docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "TRUNCATE TABLE bronze.erp_loc_a101;"
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "COPY bronze.erp_loc_a101 (cid, cntry) FROM '/tmp/source_erp/LOC_A101.csv' WITH (FORMAT CSV, HEADER TRUE, NULL '');"
echo "    erp_loc_a101 loaded"

docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "TRUNCATE TABLE bronze.erp_px_cat_g1v2;"
docker exec "$CONTAINER" psql -U "$USER" -d "$DB" -c "COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance) FROM '/tmp/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER TRUE, NULL '');"
echo "    erp_px_cat_g1v2 loaded"

echo ">>> Cleaning up temp files from container..."
docker exec "$CONTAINER" rm -rf /tmp/source_crm /tmp/source_erp

echo ">>> Done."
