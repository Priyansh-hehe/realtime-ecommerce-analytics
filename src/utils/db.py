"""
src/utils/db.py
---------------
PostgreSQL connection pool and persistence helpers using psycopg v3.

Responsibilities:
  1. Open a thread-safe connection pool on startup.
  2. Provide a single `insert_transaction()` function for the consumer.
  3. Provide a `insert_dead_letter()` function for DLQ persistence.
  4. Gracefully close the pool on shutdown.
"""

import logging
import os
from typing import Optional

import psycopg
from psycopg_pool import ConnectionPool

logger = logging.getLogger("db")

# ─── Connection string ───────────────────────────────────────────────────────
# Reads from environment variables; falls back to local Docker defaults.
_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'ecommerce')} "
    f"user={os.getenv('POSTGRES_USER', 'ecom_user')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'ecom_pass')}"
)

# Module-level pool (initialized once, reused across all inserts)
_pool: Optional[ConnectionPool] = None


# ─── Pool lifecycle ──────────────────────────────────────────────────────────

def init_pool(min_size: int = 2, max_size: int = 10) -> None:
    """
    Opens the psycopg connection pool.
    Call once at consumer startup before processing any messages.
    """
    global _pool
    _pool = ConnectionPool(conninfo=_DSN, min_size=min_size, max_size=max_size)
    _pool.wait()           # blocks until at least min_size connections are ready
    logger.info(f"PostgreSQL pool ready ({min_size}–{max_size} connections) → {_DSN.split('dbname=')[1].split()[0]}")


def close_pool() -> None:
    """Flush and close the connection pool. Call at consumer shutdown."""
    global _pool
    if _pool:
        _pool.close()
        logger.info("PostgreSQL pool closed.")


# ─── Insert helpers ──────────────────────────────────────────────────────────

_INSERT_TRANSACTION = """
    INSERT INTO transactions (
        event_id, invoice_no, stock_code, description,
        quantity, unit_price, total_amount, invoice_date,
        customer_id, country,
        event_type, is_cancellation, is_guest,
        kafka_topic, kafka_partition, kafka_offset,
        produced_at, processed_at, latency_ms
    ) VALUES (
        %(event_id)s, %(invoice_no)s, %(stock_code)s, %(description)s,
        %(quantity)s, %(unit_price)s, %(total_amount)s, %(invoice_date)s,
        %(customer_id)s, %(country)s,
        %(event_type)s, %(is_cancellation)s, %(is_guest)s,
        %(kafka_topic)s, %(kafka_partition)s, %(kafka_offset)s,
        COALESCE(%(produced_at)s::timestamptz, NOW()), %(processed_at)s, %(latency_ms)s
    )
    ON CONFLICT (event_id) DO NOTHING;
"""

_INSERT_DLQ = """
    INSERT INTO dead_letter_queue (
        dlq_id, error_reason, error_details,
        original_topic, original_partition, original_offset,
        raw_payload, rejected_at
    ) VALUES (
        %(dlq_id)s, %(error_reason)s, %(error_details)s,
        %(original_topic)s, %(original_partition)s, %(original_offset)s,
        %(raw_payload)s, %(rejected_at)s
    )
    ON CONFLICT (dlq_id) DO NOTHING;
"""


def insert_transaction(record: dict) -> None:
    """
    Persists a single validated TransactionEvent dict to the `transactions` table.
    Uses ON CONFLICT DO NOTHING so duplicate event_ids are silently skipped.

    Args:
        record: dict matching TransactionEvent fields + kafka_topic/partition/offset.
    """
    if _pool is None:
        raise RuntimeError("DB pool is not initialised. Call init_pool() first.")
    with _pool.connection() as conn:
        conn.execute(_INSERT_TRANSACTION, record)


def insert_dead_letter(record: dict) -> None:
    """
    Persists a rejected event envelope to the `dead_letter_queue` table.

    Args:
        record: dict matching DeadLetterEnvelope fields.
    """
    if _pool is None:
        raise RuntimeError("DB pool is not initialised. Call init_pool() first.")
    with _pool.connection() as conn:
        conn.execute(_INSERT_DLQ, record)
