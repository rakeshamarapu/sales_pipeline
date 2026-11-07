# Source-to-Target Mapping

This document describes how source fields flow through validation, file layers, and the PostgreSQL star schema. CSV and REST API ingestion preserve a business `source_system` value when it exists and add `ingestion_source` and `ingestion_timestamp` for pipeline provenance. PostgreSQL extraction uses `source_system=postgres` when a source row does not already provide one.

## Customer

| Source field | Clean field | Warehouse target | Notes |
| --- | --- | --- | --- |
| `customer_id` | `customer_id` | `dim_customer.customer_id` | Business key; multiple historical versions are allowed |
| `customer_name` | `customer_name` | `dim_customer.customer_name` | Tracked by SCD Type 2 |
| `email` | `email` | `dim_customer.email` | Validated format; tracked by SCD Type 2 |
| `city` | `city` | `dim_customer.city` | Tracked by SCD Type 2 |
| `state` | `state` | `dim_customer.state` | Tracked by SCD Type 2 |
| `updated_at` | UTC timestamp | `effective_start_date`, `source_updated_at` | Date starts the dimension version; exact timestamp prevents stale updates |
| Generated | N/A | `customer_key` | Surrogate primary key |
| Generated | N/A | `effective_end_date`, `is_current` | End date is exclusive; current rows have a null end date |

## Product

| Source field | Clean field | Warehouse target | Notes |
| --- | --- | --- | --- |
| `product_id` | `product_id` | `dim_product.product_id` | Business key |
| `product_name` | `product_name` | `dim_product.product_name` | Updated using Type 1 behavior |
| `category` | `category` | `dim_product.category` | Updated using Type 1 behavior |
| `price` | positive numeric `price` | `dim_product.price`, `fact_sales.unit_price` | Fact price is taken from the product row at load time |
| `updated_at` | UTC timestamp | `dim_product.source_updated_at` | Older updates do not overwrite newer product values |
| Generated | N/A | `product_key` | Surrogate primary key used by facts |

## Order / Sales Fact

| Source field | Clean field | Warehouse target | Notes |
| --- | --- | --- | --- |
| `order_id` | `order_id` | `fact_sales.order_id` | Idempotent fact business key |
| `customer_id` | `customer_id` | `fact_sales.customer_key` | Resolved to the SCD2 version effective on `order_date` |
| `product_id` | `product_id` | `fact_sales.product_key` | Resolved through `dim_product` |
| `order_date` | UTC date | `fact_sales.date_key` | Resolved through `dim_date` |
| `quantity` | positive whole number | `fact_sales.quantity` | Checked before loading |
| Product `price` | positive numeric | `fact_sales.unit_price` | Source orders do not carry a price; current product price is used |
| Derived | `quantity * unit_price` | `fact_sales.total_amount` | Rounded to two decimal places |
| `order_status` | `order_status` | `fact_sales.order_status` | Retained for status analytics |
| `source_system` | `source_system` | `fact_sales.source_system` | Business channel, such as web, mobile, or POS |
| `updated_at` | UTC timestamp | `fact_sales.source_updated_at` | Used for latest-version UPSERT and incremental extraction |
| Ingestion metadata | `ingestion_timestamp` | `fact_sales.ingestion_timestamp` | Warehouse load time |
| Generated | N/A | `sales_key` | Surrogate primary key |

## Data Layers

Raw snapshots retain source columns plus ingestion metadata. Clean snapshots contain standardized, validated records with duplicate business keys resolved to the latest valid `updated_at`. Curated sales snapshots join clean orders to customer and product attributes and calculate `unit_price` and `total_amount`.
