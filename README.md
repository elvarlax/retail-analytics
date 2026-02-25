# Retail Analytics ELT

[![Retail Analytics - ELT & dbt
CI](https://github.com/elvarlax/retail-analytics/actions/workflows/analytics-ci.yml/badge.svg)](https://github.com/elvarlax/retail-analytics/actions/workflows/analytics-ci.yml)

Analytical warehouse built on top of the `RetailInventory` OLTP system.

This project demonstrates:

-   ELT using PostgreSQL as both source and warehouse
-   Layered dbt modeling (`raw → staging → intermediate → marts`)
-   Kimball-style star schema design
-   Incremental fact tables
-   Data quality testing with dbt
-   Continuous Integration with GitHub Actions

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

-   **Raw**: Column-level structural replica of OLTP tables (types +
    nullability preserved)
-   **Staging**: Renames keys, casts types, light transforms
-   **Intermediate**: Centralised joins and derived fields
-   **Marts**: Final analytical tables (dimensions + facts)

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

### Start Warehouse

``` bash
docker compose up warehouse -d
```

### Run Extract

``` bash
docker compose up extract
```

### Build dbt Models

``` bash
docker compose run --rm dbt deps
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

Then open:

http://localhost:8081

The documentation includes:

-   Model descriptions
-   Column-level metadata
-   Test coverage
-   Data lineage graph

------------------------------------------------------------------------

## CI / Workflow

GitHub Actions runs the full pipeline on every push and pull request:

-   Spins up Postgres source and warehouse services
-   Executes the Python extract
-   Runs `dbt deps`, `dbt run`, and `dbt test`
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