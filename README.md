# Retail Omnichannel Sales Pipeline

A beginner-friendly Python and PostgreSQL data engineering project for bringing retail sales data from CSV files, a REST API, and PostgreSQL into a warehouse. The planned pipeline validates and cleans source records before loading raw, clean, and curated data layers and a PostgreSQL star schema.

## Project Status

**Phases 1 through 8 complete:** the project includes source ingestion, validation and cleaning, raw/clean/curated file layers, a PostgreSQL star schema, SCD Type 2, incremental loading, analytics, reporting, tests, and a runnable `main.py`. The default run uses local sample data and does not require PostgreSQL. Warehouse operations require a configured PostgreSQL database; live database execution has not been verified in this environment. Airflow remains an optional extension and is not required by the core pipeline.

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
src/ingestion/             CSV, REST API, and PostgreSQL readers
src/validation/            Required-field, format, date, and reference checks
src/transformation/        Cleaning, deduplication, and file-layer storage
src/warehouse/             Star schema, SCD Type 2, and setup helpers
src/loading/               Incremental dimension/fact loading and watermarks
src/reporting/             Quality, execution, and reconciliation reports
src/utils/                 Configuration, database, and logging helpers
sql/create_tables.sql      Standalone star-schema DDL
sql/analytics_queries.sql  Ten PostgreSQL analytics queries
docs/                      Source-to-target mapping
logs/                      Pipeline logs and per-run reports
tests/                     Automated test suite
main.py                    End-to-end pipeline entry point
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

## Run the Pipeline

From the activated virtual environment, run the complete local sample pipeline:

```powershell
python main.py
```

By default the runner reads the three CSV files, starts the bundled orders API on a temporary local port, validates and deduplicates all records, and writes raw, clean, and curated snapshots. The curated sales output joins customer/product names and calculates order revenue. On the included sample data, one run processes 235 source rows, produces 135 clean dimension/order rows and 100 curated sales rows, and identifies 100 duplicate order rows across the CSV and API sources.

Execution logs, a JSONL run record, JSON quality report, and CSV reconciliation report are written under `logs/`. Generated raw, clean, curated, quarantine, and log files are excluded from Git.

PostgreSQL extraction and warehouse loading are opt-in:

```powershell
python main.py --load-warehouse
python main.py --include-postgres --load-warehouse
```

Create the `retail_omnichannel` PostgreSQL database and configure `.env` first. `--include-postgres` additionally expects existing source tables named `customers`, `products`, and `orders`. `--load-warehouse` initializes/upgrades the star schema, loads customer and product dimensions, populates the date dimension, and upserts sales. Leaving `ORDERS_API_URL` unset uses the local mock; set it in `.env` only when using a reachable external API endpoint.

Run all tests with:

```powershell
pytest
```

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

Raw snapshots are written automatically by ingestion. Save validated, deduplicated data to the clean layer with `write_clean_snapshot`. The curated writer stores a prepared data frame; `main.py` joins orders to customer and product attributes and calculates revenue before writing curated sales. Each call creates a timestamped CSV in the selected layer. Supplying the same `run_id` again replaces that entity's snapshot for that run.

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

## Incremental Loading and SCD Type 2

CSV, API, and PostgreSQL readers accept an optional inclusive `updated_since` watermark. Rows on the boundary are replayed; invalid CSV/API timestamps are retained for validation rather than silently filtered out. PostgreSQL incremental extraction uses a parameterized timestamp comparison.

```python
from src.ingestion.api_ingestion import ingest_orders
from src.loading.incremental_loader import (
  DEFAULT_ORDER_PIPELINE,
  get_pipeline_watermark,
  load_incremental_orders,
)

last_loaded_at = get_pipeline_watermark(DEFAULT_ORDER_PIPELINE)
orders = ingest_orders(updated_since=last_loaded_at)
result = load_incremental_orders(orders)
print(f"Upserted {result.upserted_rows} orders")
```

`load_incremental_orders` resolves the customer version effective on the order date, product key, and date key. It upserts by `order_id` and advances the watermark in the same transaction. The database only accepts updates at least as recent as the stored source `updated_at`, so older replays cannot replace newer facts. Ensure the corresponding customer/product dimensions and `dim_date` range are loaded first.

Load dimensions before facts:

```python
from src.loading.incremental_loader import upsert_products
from src.warehouse.scd_type2 import apply_customer_changes

apply_customer_changes(customer_changes)
upsert_products(product_changes)
```

`apply_customer_changes` expires the current customer row and inserts a new version on tracked attribute changes; the exact source timestamp prevents stale same-day replays. Both operations advance their watermarks in their respective transactions. PostgreSQL connections are configured through the local `.env` file.

## Analytics and Mapping

The requested ten PostgreSQL analytics queries are in [sql/analytics_queries.sql](sql/analytics_queries.sql). They cover order detail, customer/product sales, monthly revenue, statuses, customers without sales, city totals, a CTE, and a window-function ranking. Field-level source, clean, curated, and warehouse mappings are documented in [docs/source_to_target_mapping.md](docs/source_to_target_mapping.md).

## Planned Pipeline Phases

1. Create the project scaffold and sample data. (Complete)
2. Ingest API, CSV, and PostgreSQL records. (Complete)
3. Validate, clean, and deduplicate records. (Complete)
4. Store raw, clean, and curated data. (Complete)
5. Create the PostgreSQL star schema. (Complete; live database execution pending)
6. Add SCD Type 2 and incremental loading. (Complete; live database execution pending)
7. Add analytics, logging, and reconciliation. (Complete)
8. Add automated tests and complete run documentation. (Complete)
