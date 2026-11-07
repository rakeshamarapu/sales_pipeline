# Retail Omnichannel Sales Pipeline

A beginner-friendly Python and PostgreSQL data engineering project for bringing retail sales data from CSV files, a REST API, and PostgreSQL into a warehouse. The planned pipeline validates and cleans source records before loading raw, clean, and curated data layers and a PostgreSQL star schema.

## Project Status

**Phases 1 through 5 complete:** the scaffold, sample data, source ingestion, validation and cleaning, file-based data layers, and PostgreSQL star-schema DDL are available. SCD Type 2, warehouse loading, analytics, and the end-to-end `main.py` runner will be added in later phases.

## Sample Data

The sample files are in `data/sample/`:

| File | Records | Description |
| --- | ---: | --- |
| `customers.csv` | 20 | Customer identifiers, contact details, locations, and update times |
| `products.csv` | 15 | Product identifiers, categories, prices, and update times |
| `orders.csv` | 100 | Orders referencing the sample customers and products |

Order dates and update timestamps use ISO 8601 UTC values. Order customer and product IDs refer to entries in the corresponding sample files.

## Project Structure

```text
config/                  Non-secret pipeline settings
data/                    Pipeline data folders
  raw/                   Original ingested records
  clean/                 Validated and cleaned records
  curated/               Business-ready records
  quarantine/            Rejected records
  sample/                Local CSV input data
src/ingestion/            CSV, REST API, and PostgreSQL readers
src/validation/           Required-field, format, date, and reference checks
src/transformation/       Cleaning, deduplication, and layer snapshot storage
src/warehouse/            Star-schema DDL and PostgreSQL setup helpers
sql/create_tables.sql     Standalone copy of the star-schema DDL
src/utils/                Configuration and database connection helpers
sql/                      Database DDL and analytics SQL (later phases)
logs/                     Pipeline execution logs
tests/                    Ingestion tests
requirements.txt          Python dependencies
.env.example              Local environment variable template
```

## Windows Setup

Use PowerShell from the project directory:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

PostgreSQL credentials are not stored in the repository. Copy `.env.example` to `.env` and replace its example values with credentials for your local PostgreSQL instance before extracting from PostgreSQL. The `.env` file is excluded from Git.

## Configuration

`config/config.yaml` contains non-secret paths, API settings, and database environment-variable names. Actual credentials belong in the local `.env` file, never in the YAML file or source code.

## Run Ingestion

Run the local sample API in one PowerShell terminal:

```powershell
python -m src.ingestion.mock_api
```

In a second terminal, ingest the CSV sample files or call the mock API:

```powershell
python -c "from src.ingestion.csv_ingestion import ingest_sample_csvs; print({name: len(data) for name, data in ingest_sample_csvs().items()})"
python -c "from src.ingestion.api_ingestion import ingest_orders; print(len(ingest_orders()))"
```

Each source writes timestamped CSV snapshots under `data/raw/`. The API reader uses `ORDERS_API_URL` when set and otherwise calls the local mock API. PostgreSQL extraction reads the `customers`, `products`, or `orders` table and requires the corresponding settings in `.env`:

```powershell
python -c "from src.ingestion.postgres_ingestion import extract_table; print(len(extract_table('orders')))"
```

Run the current tests with `pytest`.

## Validate and Clean Sample Data

Ingestion functions return data frames. Order validation needs the customer and product IDs that passed their own checks. Invalid records are written to `data/quarantine/`; the cleaning functions return deduplicated data frames.

```python
import pandas as pd

from src.transformation.data_cleaner import clean_and_deduplicate

customers = clean_and_deduplicate(
  pd.read_csv("data/sample/customers.csv"), "customers"
).clean_records
products = clean_and_deduplicate(
  pd.read_csv("data/sample/products.csv"), "products"
).clean_records
orders = clean_and_deduplicate(
  pd.read_csv("data/sample/orders.csv"),
  "orders",
  customer_ids=set(customers["customer_id"]),
  product_ids=set(products["product_id"]),
).clean_records
```

## Save Clean and Curated Snapshots

Raw snapshots are written automatically by ingestion. Save validated, deduplicated data to the clean layer with `write_clean_snapshot`. The curated writer stores a prepared data frame; business-specific curated transformations are a later step. Each call creates a timestamped CSV in the selected layer. Supplying the same `run_id` again replaces that entity's snapshot for that run.

```python
from src.transformation.data_cleaner import clean_and_deduplicate
from src.transformation.data_layers import (
  read_latest_snapshot,
  write_clean_snapshot,
  write_curated_snapshot,
)

cleaning_result = clean_and_deduplicate(
  pd.read_csv("data/sample/customers.csv"), "customers"
)
run_id = "sample-20250101"
write_clean_snapshot(cleaning_result.clean_records, "customers", run_id=run_id)
write_curated_snapshot(cleaning_result.clean_records, "customers", run_id=run_id)
latest_customers = read_latest_snapshot("clean", "customers")
```

## Initialize the PostgreSQL Star Schema

Create the PostgreSQL database first, then set its connection values in `.env`. The schema initializer creates `dim_customer`, `dim_product`, `dim_date`, and `fact_sales` with primary keys, foreign keys, checks, and indexes. It is safe to rerun. Populate the date dimension with the inclusive date range needed by your sales data:

```python
from datetime import date

from src.warehouse.schema_manager import (
  initialize_star_schema,
  populate_date_dimension,
)

initialize_star_schema()
inserted_dates = populate_date_dimension(date(2025, 1, 1), date(2025, 12, 31))
print(f"Inserted {inserted_dates} date rows")
```

The date dimension uses `YYYYMMDD` integer keys and ignores dates already loaded. The schema files are [src/warehouse/star_schema.sql](src/warehouse/star_schema.sql) and [sql/create_tables.sql](sql/create_tables.sql); the tests ensure they stay identical. PostgreSQL execution requires a reachable server and database and has not been run in the current development environment.

## Planned Pipeline Phases

1. Create the project scaffold and sample data. (Complete)
2. Ingest API, CSV, and PostgreSQL records. (Complete)
3. Validate, clean, and deduplicate records. (Complete)
4. Store raw, clean, and curated data. (Complete)
5. Create the PostgreSQL star schema. (Complete; live database execution pending)
6. Add SCD Type 2 and incremental loading.
7. Add analytics, logging, and reconciliation.
8. Add automated tests and complete run documentation.
