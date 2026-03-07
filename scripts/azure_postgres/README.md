# Azure Postgres - CRM & ERP Data Loader

Loads CSV files from `data/engineering/` into an Azure PostgreSQL database called `company_data`.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed
- `.env` file at project root with `AZURE_POSTGRES_URL` set

## Usage

From the project root:

```bash
uv run scripts/azure_postgres/load_to_postgres.py
```

No virtual environment or manual dependency install needed — uv handles it via inline script metadata.

## What it does

1. Connects to the Azure PostgreSQL server using `AZURE_POSTGRES_URL`
2. Creates the `company_data` database if it doesn't exist
3. Reads 6 CSV files and loads them into tables:

| Source | CSV File | Table Name | Rows |
|--------|----------|------------|------|
| CRM | `source_crm/cust_info.csv` | `crm_cust_info` | 18,494 |
| CRM | `source_crm/prd_info.csv` | `crm_prd_info` | 397 |
| CRM | `source_crm/sales_details.csv` | `crm_sales_details` | 60,398 |
| ERP | `source_erp/CUST_AZ12.csv` | `erp_cust_az12` | 18,484 |
| ERP | `source_erp/PX_CAT_G1V2.csv` | `erp_px_cat_g1v2` | 37 |
| ERP | `source_erp/LOC_A101.csv` | `erp_loc_a101` | 18,484 |

Tables are replaced on each run (`if_exists="replace"`).

## Verify

```bash
uv run --with psycopg --with python-dotenv python3 -c "
import os, psycopg
from dotenv import load_dotenv
load_dotenv()
url = os.environ['AZURE_POSTGRES_URL'].replace('postgresql+psycopg://', 'postgresql://')
url = url.rsplit('/', 1)[0] + '/company_data?' + url.rsplit('?', 1)[1]
conn = psycopg.connect(url)
cur = conn.cursor()
cur.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='public'\")
for t in cur.fetchall():
    cur.execute(f'SELECT COUNT(*) FROM {t[0]}')
    print(f'{t[0]:30s} {cur.fetchone()[0]:>7,} rows')
conn.close()
"
```
