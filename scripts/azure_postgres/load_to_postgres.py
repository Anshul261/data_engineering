# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pandas",
#     "python-dotenv",
#     "sqlalchemy",
#     "psycopg[binary]",
# ]
# ///
"""Load CRM and ERP CSV data into Azure PostgreSQL company_data database."""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "engineering")

# Parse the connection URL to build company_data URL
base_url = os.environ["AZURE_POSTGRES_URL"]
server_url = base_url.rsplit("/", 1)[0]
db_params = base_url.rsplit("/", 1)[1]
params = ""
if "?" in db_params:
    params = "?" + db_params.split("?", 1)[1]

company_data_url = f"{server_url}/company_data{params}"

# Step 1: Create the company_data database if it doesn't exist
print("Connecting to server to create company_data database...")
engine_default = create_engine(base_url, isolation_level="AUTOCOMMIT")
with engine_default.connect() as conn:
    exists = conn.execute(
        text("SELECT 1 FROM pg_database WHERE datname = 'company_data'")
    ).fetchone()
    if not exists:
        conn.execute(text("CREATE DATABASE company_data"))
        print("Created database: company_data")
    else:
        print("Database company_data already exists")
engine_default.dispose()

# Step 2: Connect to company_data and load tables
print("Connecting to company_data...")
engine = create_engine(company_data_url)

csv_files = {
    "source_crm/cust_info.csv": "crm_cust_info",
    "source_crm/prd_info.csv": "crm_prd_info",
    "source_crm/sales_details.csv": "crm_sales_details",
    "source_erp/CUST_AZ12.csv": "erp_cust_az12",
    "source_erp/PX_CAT_G1V2.csv": "erp_px_cat_g1v2",
    "source_erp/LOC_A101.csv": "erp_loc_a101",
}

for csv_path, table_name in csv_files.items():
    full_path = os.path.join(DATA_DIR, csv_path)
    print(f"Loading {csv_path} -> {table_name}...")
    df = pd.read_csv(full_path)
    df.columns = df.columns.str.strip().str.lower()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].str.strip()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"  Loaded {len(df)} rows")

engine.dispose()
print("\nDone! All CRM and ERP data loaded into company_data database.")
