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

WAREHOUSE_DSN = (
    f"host={os.getenv('WAREHOUSE_DB_HOST')} "
    f"port={os.getenv('WAREHOUSE_DB_PORT')} "
    f"dbname={os.getenv('WAREHOUSE_DB_NAME')} "
    f"user={os.getenv('WAREHOUSE_DB_USER')} "
    f"password={os.getenv('WAREHOUSE_DB_PASSWORD')}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def setup_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS events")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events.retail_events (
                id               BIGSERIAL   PRIMARY KEY,
                event_type       TEXT        NOT NULL,
                payload          JSONB       NOT NULL,
                source           TEXT,
                occurred_at_utc  TIMESTAMPTZ,
                received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    conn.commit()
    log.info("Events schema ready.")


def handle_message(event_type: str, payload: dict, app_properties: dict, cur):
    source          = app_properties.get("source")
    occurred_at_utc = payload.get("OccurredAtUtc")

    cur.execute(
        """
        INSERT INTO events.retail_events (event_type, payload, source, occurred_at_utc)
        VALUES (%s, %s, %s, %s)
        """,
        (event_type, json.dumps(payload), source, occurred_at_utc),
    )
    log.info("Stored %s", event_type)


KNOWN_EVENT_TYPES = {
    "CustomerCreatedV1",
    "ProductCreatedV1",
    "OrderPlacedV1",
    "OrderStatusChangedV1",
}


def run():
    log.info("Consumer starting - topic=%s subscription=%s", TOPIC_NAME, SUBSCRIPTION_NAME)

    with psycopg.connect(WAREHOUSE_DSN, autocommit=False) as conn:
        setup_schema(conn)

        with ServiceBusClient.from_connection_string(SERVICE_BUS_CONN_STR) as sb_client:
            with sb_client.get_subscription_receiver(
                topic_name=TOPIC_NAME,
                subscription_name=SUBSCRIPTION_NAME,
            ) as receiver:
                log.info("Listening for messages...")

                for msg in receiver:
                    event_type = msg.subject or "unknown"

                    if event_type not in KNOWN_EVENT_TYPES:
                        log.warning("Unknown event type '%s' - skipping", event_type)
                        receiver.complete_message(msg)
                        continue

                    try:
                        payload = json.loads(str(msg))
                        app_properties = msg.application_properties or {}
                        with conn.cursor() as cur:
                            handle_message(event_type, payload, app_properties, cur)
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


if __name__ == "__main__":
    run()
