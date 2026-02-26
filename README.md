# Retail Analytics ELT

[![Retail Analytics - ELT & dbt CI](https://github.com/elvarlax/retail-analytics/actions/workflows/analytics-ci.yml/badge.svg)](https://github.com/elvarlax/retail-analytics/actions/workflows/analytics-ci.yml)

Retail Analytics is an ELT pipeline and analytical warehouse built on top of the `RetailInventory` OLTP system.

The project demonstrates end-to-end data engineering: streaming extraction from a live Postgres source, full-refresh loading into a raw layer, layered dbt transformations into a Kimball star schema, data quality testing across every layer, and a CI pipeline that runs the full pipeline on every push. It reflects pragmatic data warehouse design rather than tutorial-level ETL.

------------------------------------------------------------------------

## Architecture

    retail-inventory OLTP       retail-analytics OLAP
    (Postgres :5433)           (Postgres :5434)
            │                           │
            └── Python extract ───────▶│ raw.*
                                            │
                                            │ dbt
                                            │── staging.*
                                            │── intermediate.*
                                            └── marts.* (star schema)

### Layers

-   **Raw**: Column-level structural replica of OLTP tables — schema
    introspected at runtime from `pg_catalog`, types and nullability
    preserved exactly
-   **Staging**: Renames keys, casts types, light transforms
-   **Intermediate**: Centralised joins and derived fields
-   **Marts**: Final analytical tables (dimensions + facts)

------------------------------------------------------------------------

## Extract Design

The extract script reads from `retail_inventory` and writes to the `raw`
schema in `retail_warehouse`. Each table is a full refresh — drop,
recreate, reload.

**Schema introspection via `pg_catalog`**
DDL for each raw table is generated at runtime by querying
`pg_catalog.pg_attribute`. Column names, data types, and nullability are
read directly from the source — no hardcoded `CREATE TABLE` statements.
Schema changes in the OLTP propagate automatically on the next run.

**Streaming COPY**
Data is transferred using PostgreSQL `COPY TO STDOUT / FROM STDIN` in
CSV format. Rows stream in chunks between the source and target
connections without buffering the full table in memory. Suitable for
large tables.

**Transactional safety**
All tables load within a single database transaction. Each table's
DROP, CREATE, and COPY are wrapped in a savepoint — if COPY fails
mid-stream, that table's changes are rolled back and the pipeline
fails fast. Because all tables share one connection's outer
transaction, a failed run leaves the raw schema in its pre-run state.

------------------------------------------------------------------------

## Star Schema

                            ┌──▶ fact_order_items (line grain)
    dim_date ───────┐       │
    dim_customers ──┼───────┤
    dim_products ───┘       │
                            └──▶ fact_orders (order grain)

-   `fact_orders`: One row per order
-   `fact_order_items`: One row per line item
-   Dimensions: `dim_date`, `dim_customers`, `dim_products`

------------------------------------------------------------------------

## Dataset

Generated via the `retail-inventory` admin data generator. Customers, products, and orders are seeded directly, order items are generated per order.

| Table        | Rows    |
|--------------|---------|
| customers    | 10,000  |
| products     | 1,000   |
| orders       | 100,000 |
| order_items  | 250,000 |

------------------------------------------------------------------------

## Testing Strategy

89 data tests across all four layers.

**Staging** — source data quality:
`unique`, `not_null` on all primary and natural keys, `accepted_values`
for order status, `expression_is_true` for positive prices and quantities.

**Intermediate** — join correctness:
`unique` and `not_null` on PKs to catch fan-outs or dropped rows from
joins. `not_null` on date columns to validate derivation logic.
`expression_is_true` on `line_amount` and `total_amount`.

**Marts** — consumption layer:
FK `relationships` to all three dimensions, `accepted_values` for
status, `expression_is_true` on measures, row count bounds via
`dbt_expectations.expect_table_row_count_to_be_between`.

**Singular tests:**
`assert_positive_line_amounts` — every order line must have
`line_amount > 0`.
`assert_completed_at_after_created_at` — completed orders must have
`completed_at >= created_at`.

------------------------------------------------------------------------

## Example Queries

### Monthly Revenue & Average Order Value

``` sql
select
    d.year,
    d.month,
    count(*) as total_orders,
    sum(f.total_amount) as revenue,
    round(avg(f.total_amount)::numeric, 2) as avg_order_value
from marts.fact_orders f
join marts.dim_date d on f.date_key = d.date_key
where f.is_completed
group by 1, 2
order by 1, 2;
```

------------------------------------------------------------------------

### Top Products by Revenue

``` sql
select
    p.product_name,
    p.sku,
    sum(f.line_amount) as revenue,
    sum(f.quantity) as units_sold
from marts.fact_order_items f
join marts.dim_products p using (product_id)
where f.is_completed
group by 1, 2
order by revenue desc
limit 10;
```

------------------------------------------------------------------------

## Running Locally

### Prerequisites

-   Docker & Docker Compose
-   The [retail-inventory](https://github.com/elvarlax/retail-inventory)
    OLTP database must be running on `localhost:5433`

### Start Warehouse

``` bash
docker compose up warehouse -d
```

### Run Extract

``` bash
docker compose up extract
```

### Install dbt Packages (first run only)

``` bash
docker compose run --rm dbt deps
```

### Build dbt Models & Run Tests

``` bash
docker compose run --rm dbt build
```

Or separately during development:

``` bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

### Generate dbt Documentation

``` bash
docker compose run --rm dbt docs generate
```

### Serve Documentation

``` bash
docker compose run --rm -p 8081:8080 dbt docs serve --host 0.0.0.0 --port 8080
```

Then open: http://localhost:8081

The documentation includes model descriptions, column-level metadata,
test coverage, and the data lineage graph.

------------------------------------------------------------------------

## CI / Workflow

GitHub Actions runs the full pipeline on every push and pull request:

-   Spins up Postgres source and warehouse services
-   Applies EF Core migrations to create the OLTP schema
-   Executes the Python extract
-   Runs `dbt deps` and `dbt build` (models + tests)
-   Fails on any extraction or transformation errors

Workflow file: `.github/workflows/analytics-ci.yml`

------------------------------------------------------------------------

## Tech Stack

-   Python 3.11
-   PostgreSQL 16
-   psycopg v3
-   dbt-postgres
-   dbt-utils
-   dbt-expectations
-   Docker & Docker Compose
-   GitHub Actions (CI)
