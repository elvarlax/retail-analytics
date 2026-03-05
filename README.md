# Retail Analytics Event-Driven Warehouse

[![Retail Analytics - Event-Driven dbt CI](https://github.com/elvarlax/retail-analytics/actions/workflows/analytics-ci.yml/badge.svg)](https://github.com/elvarlax/retail-analytics/actions/workflows/analytics-ci.yml)

Retail Analytics is an event-driven analytical warehouse for the [retail-inventory](https://github.com/elvarlax/retail-inventory) domain.
The source system publishes business events to Azure Service Bus, this project consumes those events into Postgres, and dbt builds a star schema for analytics.

------------------------------------------------------------------------

## Architecture

    Azure Service Bus topic (retail.events)
                    |
                    v
          Python consumer (consumer/consumer.py)
                    |
                    v
    events.retail_events (raw event log, JSONB payload)
                    |
                    v
            dbt models (staging -> intermediate -> marts)
                    |
                    v
          marts.* facts and dimensions for BI/analysis

### Warehouse layers

| Layer          | Purpose |
|----------------|---------|
| `events`       | Raw event ingestion log (`events.retail_events`) |
| `staging`      | Typed fields extracted from raw payloads |
| `intermediate` | Reusable joins and business logic |
| `marts`        | Final star schema (facts + dimensions) |

------------------------------------------------------------------------

## Event Consumer

The consumer listens on:
- Topic: `retail.events`
- Subscription: `analytics-sub`

Supported event types:
- `CustomerCreatedV1`
- `ProductCreatedV1`
- `OrderPlacedV1`
- `OrderStatusChangedV1`

Behavior:
- Creates the `events` schema and `events.retail_events` table if missing
- Inserts one row per message with event type, payload, source, and timestamps
- Completes messages on success
- Abandons failed messages for retry
- Dead-letters messages when delivery count reaches `MAX_RETRIES`

------------------------------------------------------------------------

## Star Schema

Core marts:
- `marts.fact_orders` (order grain)
- `marts.fact_order_items` (line-item grain)
- `marts.dim_customers`
- `marts.dim_products`
- `marts.dim_date`

Example analysis use cases:
- Revenue trend and average order value
- Product performance by revenue and units sold
- Customer-level ordering behavior

------------------------------------------------------------------------

## Data Quality

dbt tests cover schema integrity and business rules across all layers:
- Key integrity: `unique`, `not_null`, and FK `relationships`
- Domain checks: `accepted_values` for order statuses
- Metric checks: positive amount/quantity expressions
- Business-rule singular tests:
  - `assert_completed_at_after_created_at`
  - `assert_fact_orders_completion_consistency`
  - `assert_positive_line_amounts`
  - `assert_retail_events_recency` (freshness guard for ingestion)

Recency threshold can be overridden:

```bash
docker compose --profile dbt run --rm dbt build --vars "{events_recency_hours: 24}"
```

------------------------------------------------------------------------

## Running Locally

### Prerequisites

- Docker and Docker Compose
- Azure Service Bus connection string (cloud or emulator)

Create `.env` in the repo root:

```env
SERVICE_BUS_CONNECTION_STRING=Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>=;UseDevelopmentEmulator=true;
SERVICE_BUS_TOPIC=retail.events
SERVICE_BUS_SUBSCRIPTION=analytics-sub
MAX_RETRIES=3
```

### Start warehouse

```bash
docker compose up warehouse -d
```

### Start event consumer

```bash
docker compose up consumer
```

### Build dbt models

```bash
docker compose run --rm dbt deps
docker compose run --rm dbt build
```

During development:

```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

### Generate docs

```bash
docker compose run --rm dbt docs generate
docker compose run --rm -p 8081:8080 dbt docs serve --host 0.0.0.0 --port 8080
```

Open: `http://localhost:8081`

------------------------------------------------------------------------

## CI / Workflow

GitHub Actions workflow: `.github/workflows/analytics-ci.yml`

Pipeline steps:
- Starts Postgres warehouse service
- Installs Python dependencies
- Initializes `events.retail_events`
- Runs `dbt deps`
- Runs `dbt build`

------------------------------------------------------------------------

## Tech Stack

- Python 3.11
- PostgreSQL 16
- Azure Service Bus SDK (`azure-servicebus`)
- psycopg v3
- dbt-postgres
- dbt-utils
- dbt-expectations
- Docker and Docker Compose
- GitHub Actions
