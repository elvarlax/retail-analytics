import json
import logging
import os
import sys
import psycopg
from azure.servicebus import ServiceBusClient
from dotenv import load_dotenv

load_dotenv()

SERVICE_BUS_CONN_STR = os.getenv("SERVICE_BUS_CONNECTION_STRING")
TOPIC_NAME           = os.getenv("SERVICE_BUS_TOPIC", "retail.events")
SUBSCRIPTION_NAME    = os.getenv("SERVICE_BUS_SUBSCRIPTION", "analytics-sub")
MAX_RETRIES          = int(os.getenv("MAX_RETRIES", "3"))

KNOWN_EVENT_TYPES = {
    "CustomerCreatedV1",
    "ProductCreatedV1",
    "OrderPlacedV1",
    "OrderStatusChangedV1",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def build_warehouse_dsn() -> str:
    return (
        f"host={os.getenv('WAREHOUSE_DB_HOST')} "
        f"port={os.getenv('WAREHOUSE_DB_PORT')} "
        f"dbname={os.getenv('WAREHOUSE_DB_NAME')} "
        f"user={os.getenv('WAREHOUSE_DB_USER')} "
        f"password={os.getenv('WAREHOUSE_DB_PASSWORD')}"
    )


def setup_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS events")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events.retail_events
            (
                event_id         UUID        PRIMARY KEY,
                event_type       TEXT        NOT NULL,
                payload          JSONB       NOT NULL,
                source           TEXT,
                occurred_at_utc  TIMESTAMPTZ,
                received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_retail_events_occurred_at_utc
            ON events.retail_events (occurred_at_utc)
        """)
    conn.commit()
    log.info("Events schema ready.")


def decode_app_properties(props: dict) -> dict:
    """AMQP may return keys and values as bytes — normalize to str."""
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in props.items()
    }


def store_event(event_type: str, payload: dict, app_properties: dict, cur):
    event_id = payload.get("EventId")
    if not event_id:
        raise ValueError("Missing EventId in payload")

    source = app_properties.get("source") or "unknown"
    occurred_at_utc = payload.get("OccurredAtUtc")

    cur.execute(
        """
        INSERT INTO events.retail_events (event_id, event_type, payload, source, occurred_at_utc)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (event_id, event_type, json.dumps(payload), source, occurred_at_utc),
    )

    log.info("Stored %s (%s) from %s", event_type, event_id, source)


def process_message(msg, conn, receiver):
    event_type = msg.subject or "unknown"

    if event_type not in KNOWN_EVENT_TYPES:
        log.warning("Unknown event type '%s' - skipping", event_type)
        receiver.complete_message(msg)
        return

    try:
        payload = json.loads(str(msg))
        app_properties = decode_app_properties(msg.application_properties or {})

        with conn.cursor() as cur:
            store_event(event_type, payload, app_properties, cur)

        conn.commit()
        receiver.complete_message(msg)

    except Exception as exc:
        conn.rollback()
        log.error("Failed to process %s (delivery %d): %s", event_type, msg.delivery_count, exc)

        if msg.delivery_count >= MAX_RETRIES:
            log.warning("Dead-lettering message after %d deliveries", msg.delivery_count)
            receiver.dead_letter_message(msg, reason="MaxRetriesExceeded", error_description=str(exc))
        else:
            receiver.abandon_message(msg)


def run():
    log.info("Consumer starting - topic=%s subscription=%s", TOPIC_NAME, SUBSCRIPTION_NAME)

    with psycopg.connect(build_warehouse_dsn(), autocommit=False) as conn:
        setup_schema(conn)

        with ServiceBusClient.from_connection_string(SERVICE_BUS_CONN_STR) as sb_client:
            with sb_client.get_subscription_receiver(
                topic_name=TOPIC_NAME,
                subscription_name=SUBSCRIPTION_NAME,
            ) as receiver:
                log.info("Listening for messages...")

                for msg in receiver:
                    process_message(msg, conn, receiver)


if __name__ == "__main__":
    run()
