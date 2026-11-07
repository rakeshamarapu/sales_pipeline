# Retail Omnichannel Sales Pipeline

A beginner-friendly Python and PostgreSQL data engineering project for bringing retail sales data from CSV files, a REST API, and PostgreSQL into a warehouse. The planned pipeline validates and cleans source records before loading raw, clean, and curated data layers and a PostgreSQL star schema.

## Project Status

**Phase 1 complete:** project folders, configuration examples, Python dependencies, and sample CSV datasets are in place. Ingestion, validation, transformations, warehouse loading, analytics, and the executable pipeline will be added in later phases. As a result, `python main.py` is not available yet.

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
config/                 Non-secret pipeline settings
 data/                   Pipeline data folders
   raw/                  Original ingested records
   clean/                Validated and cleaned records
   curated/              Business-ready records
   quarantine/           Rejected records
   sample/               Local CSV input data
src/                     Pipeline Python modules (added in later phases)
sql/                     Database DDL and analytics SQL (later phases)
logs/                    Pipeline execution logs
 tests/                  Automated tests (later phases)
requirements.txt         Python dependencies
.env.example             Local environment variable template
```

## Windows Setup

Use PowerShell from the project directory:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The sample CSV files can be reviewed with a spreadsheet application or Python. PostgreSQL credentials are not stored in the repository. When database connectivity is implemented, copy `.env.example` to `.env` and replace its example values with credentials for your local PostgreSQL instance. The `.env` file is excluded from Git.

## Configuration

`config/config.yaml` contains non-secret paths and settings. Database variable names are listed there; actual credentials belong in the local `.env` file, never in the YAML file or source code.

## Planned Pipeline Phases

1. Create the project scaffold and sample data.
2. Ingest API, CSV, and PostgreSQL records.
3. Validate, clean, and deduplicate records.
4. Store raw, clean, and curated data.
5. Create the PostgreSQL star schema.
6. Add SCD Type 2 and incremental loading.
7. Add analytics, logging, and reconciliation.
8. Add automated tests and complete run documentation.
