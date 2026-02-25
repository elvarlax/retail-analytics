"""
Streaming ELT extractor for Retail Analytics.

- Reads tables from operational Postgres (retail_inventory)
- Recreates matching column structure in warehouse raw schema
- Streams data using PostgreSQL COPY (no in-memory buffering)
- Full refresh per run (raw layer only)
"""

import os
import psycopg
from dotenv import load_dotenv


# ---------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------
# Keeps connection details out of code and compatible with Docker.
load_dotenv()

SOURCE_DSN = (
    f"host={os.getenv('SOURCE_DB_HOST')} "
    f"port={os.getenv('SOURCE_DB_PORT')} "
    f"dbname={os.getenv('SOURCE_DB_NAME')} "
    f"user={os.getenv('SOURCE_DB_USER')} "
    f"password={os.getenv('SOURCE_DB_PASSWORD')}"
)

TARGET_DSN = (
    f"host={os.getenv('TARGET_DB_HOST')} "
    f"port={os.getenv('TARGET_DB_PORT')} "
    f"dbname={os.getenv('TARGET_DB_NAME')} "
    f"user={os.getenv('TARGET_DB_USER')} "
    f"password={os.getenv('TARGET_DB_PASSWORD')}"
)

# Tables listed in dependency order.
# Raw layer does not enforce FKs, but ordering keeps things predictable.
TABLES = ["customers", "products", "orders", "order_items"]


# ---------------------------------------------------------------------
# Build CREATE TABLE DDL from pg_catalog
# ---------------------------------------------------------------------
def build_create_ddl(table_name: str, source_conn) -> str:
    """
    Reconstruct column definitions from the source table using
    pg_catalog.format_type to preserve exact data types.

    Constraints and indexes are intentionally excluded in raw.
    """

    query = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            a.attnotnull AS not_null
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relname = %s
          AND n.nspname = 'public'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum;
    """

    with source_conn.cursor() as cur:
        cur.execute(query, (table_name,))
        columns = cur.fetchall()

    if not columns:
        raise RuntimeError(f"No columns found for table '{table_name}'")

    column_defs = []
    for name, dtype, not_null in columns:
        null_clause = "NOT NULL" if not_null else ""
        column_defs.append(f'  "{name}" {dtype} {null_clause}'.rstrip())

    cols_sql = ",\n".join(column_defs)

    return f'CREATE TABLE raw."{table_name}" (\n{cols_sql}\n);'


# ---------------------------------------------------------------------
# Full-refresh load (atomic per table)
# ---------------------------------------------------------------------
def load_table(table_name: str, source_conn, target_conn):
    """
    Drop and recreate the raw table, then stream data via COPY.

    Each table load runs inside its own transaction. If COPY fails,
    the drop/create is rolled back automatically.
    """

    print(f"[{table_name}] Starting load...")

    try:
        create_ddl = build_create_ddl(table_name, source_conn)

        # Transaction ensures atomicity for this table.
        with target_conn.transaction():

            with target_conn.cursor() as target_cur:
                target_cur.execute(
                    f'DROP TABLE IF EXISTS raw."{table_name}" CASCADE;'
                )
                target_cur.execute(create_ddl)

            print(f"[{table_name}] Streaming COPY...")

            # Stream data directly between source and target.
            # COPY processes data in chunks and avoids full buffering in memory.
            with source_conn.cursor() as source_cur, \
                 target_conn.cursor() as target_cur:

                with source_cur.copy(
                    f'COPY public."{table_name}" TO STDOUT WITH (FORMAT CSV)'
                ) as copy_out:

                    with target_cur.copy(
                        f'COPY raw."{table_name}" FROM STDIN WITH (FORMAT CSV)'
                    ) as copy_in:

                        for chunk in copy_out:
                            copy_in.write(chunk)

        # Basic row count check after successful commit.
        with target_conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM raw."{table_name}"')
            row_count = cur.fetchone()[0]

        print(f"[{table_name}] Loaded successfully ({row_count} rows)")

    except Exception as e:
        print(f"[{table_name}] FAILED: {e}")
        raise


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == "__main__":

    print("=" * 55)
    print("Retail Analytics — ELT extraction starting")
    print("=" * 55)

    with psycopg.connect(SOURCE_DSN) as source_conn, \
         psycopg.connect(TARGET_DSN) as target_conn:

        # Ensure raw schema exists before loading tables.
        with target_conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        for table in TABLES:
            load_table(table, source_conn, target_conn)

    print("=" * 55)
    print("ELT extraction completed successfully.")
    print("=" * 55)