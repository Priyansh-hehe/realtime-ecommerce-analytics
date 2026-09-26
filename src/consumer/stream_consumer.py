"""
src/consumer/stream_consumer.py
-------------------------------
Stream Consumer, Pydantic Schema Validator, Dead-Letter Queue (DLQ) Router,
and PostgreSQL Persistence Layer.

Responsibilities:
1. Subscribes to `ecommerce-events` topic.
2. Validates & cleanses JSON payloads using Pydantic v2 (`TransactionEvent`).
3. Classifies transaction types (PURCHASE, RETURN / CANCELLATION, PROMO_FREE, GUEST).
4. Routes malformed or invalid business records to `ecommerce-dead-letter` topic using `DeadLetterEnvelope`.
5. Persists every valid event to PostgreSQL `transactions` table.
6. Persists every rejected event to PostgreSQL `dead_letter_queue` table (dual-write with Kafka DLQ).
7. Manages manual offset commits for reliable processing guarantees.
"""

import os
import sys
import json
import uuid
import time
import signal
import logging
import argparse
from typing import Tuple, Optional, Any
from datetime import datetime, timezone
from pathlib import Path

# Allow imports from src/utils without package install
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError
from src.utils import db

# Ensure clean UTF-8 console output for Windows terminal
if sys.platform == "win32":
    getattr(sys.stdout, "reconfigure", lambda **kw: None)(encoding="utf-8")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("stream_consumer")

# Graceful shutdown flag
running = True


def handle_sigint(signum, frame):
    """Graceful shutdown handler for Ctrl+C."""
    global running
    print("\n")
    logger.info("🛑 Interrupt signal (Ctrl+C) received. Closing consumer...")
    running = False


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


# ==========================================
# Pydantic Validation & DLQ Schemas
# ==========================================

class TransactionEvent(BaseModel):
    """Validated, cleansed e-commerce transaction event."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    invoice_no: str = Field(..., min_length=1, description="Invoice identifier")
    stock_code: str = Field(..., min_length=1, description="Product SKU")
    description: Optional[str] = Field(default="NO DESCRIPTION", description="Product description")
    quantity: int = Field(..., description="Quantity ordered")
    unit_price: float = Field(..., ge=0.0, description="Unit price (must be >= 0.0)")
    total_amount: Optional[float] = Field(default=None, description="Computed: quantity * unit_price")
    invoice_date: str = Field(..., min_length=1, description="Transaction date/time string")
    customer_id: Optional[int] = Field(default=None, description="Customer ID or None for guest")
    country: str = Field(default="Unknown", description="Country of purchase")
    
    # Pipeline-derived attributes
    event_type: str = Field(default="PURCHASE", description="PURCHASE, RETURN, or PROMO_FREE")
    is_cancellation: bool = Field(default=False, description="True if invoice starts with C or quantity < 0")
    is_guest: bool = Field(default=False, description="True if customer_id is None")
    processed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @field_validator("invoice_no", "stock_code", mode="before")
    @classmethod
    def clean_strings(cls, v: Any) -> str:
        if v is None:
            raise ValueError("Field cannot be null")
        s = str(v).strip()
        if not s:
            raise ValueError("Field cannot be empty string")
        return s

    @field_validator("description", mode="before")
    @classmethod
    def clean_description(cls, v: Any) -> str:
        if v is None or str(v).strip().lower() in ("nan", "none", "null", ""):
            return "NO DESCRIPTION"
        return str(v).strip()

    @field_validator("customer_id", mode="before")
    @classmethod
    def clean_customer_id(cls, v: Any) -> Optional[int]:
        if v is None or str(v).strip().lower() in ("nan", "none", "null", ""):
            return None
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None

    @model_validator(mode="after")
    def compute_derived_fields(self) -> "TransactionEvent":
        self.total_amount = round(self.quantity * self.unit_price, 2)
        if self.invoice_no.upper().startswith("C") or self.quantity < 0:
            self.is_cancellation = True
            self.event_type = "RETURN"
        elif self.unit_price == 0.0:
            self.event_type = "PROMO_FREE"
        else:
            self.event_type = "PURCHASE"

        self.is_guest = self.customer_id is None
        return self


class DeadLetterEnvelope(BaseModel):
    """Schema for rejected/corrupt messages routed to DLQ."""
    dlq_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    error_reason: str
    error_details: Optional[str] = None
    original_topic: str
    original_partition: int
    original_offset: int
    raw_payload: str
    rejected_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ==========================================
# Consumer Metrics & Execution
# ==========================================

class ConsumerMetrics:
    """Tracks consumer performance and validation statistics."""
    def __init__(self):
        self.total_received = 0
        self.valid_count = 0
        self.dlq_count = 0
        self.purchase_count = 0
        self.return_count = 0
        self.guest_count = 0
        self.start_time = time.time()

    def print_progress(self, event_type: str, invoice_no: str, amount: float):
        elapsed = time.time() - self.start_time
        rate = self.total_received / elapsed if elapsed > 0 else 0
        logger.info(
            f">> [Consumer] Recv: {self.total_received:>6,} | "
            f"Valid: {self.valid_count:>6,} | "
            f"DLQ: {self.dlq_count:>3} | "
            f"Rate: {rate:>5.1f}/s | "
            f"[{event_type:<10}] Inv: {invoice_no:<8} | "
            f"${amount:>7.2f}"
        )


def validate_and_cleanse_event(raw_payload: str) -> Tuple[bool, Optional[TransactionEvent], Optional[str], Optional[str]]:
    """
    Validates and cleanses raw incoming event JSON using Pydantic v2.
    """
    try:
        event = TransactionEvent.model_validate_json(raw_payload)
        return True, event, None, None
    except ValidationError as ve:
        errors = [f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in ve.errors()]
        error_details = " | ".join(errors)
        return False, None, "SCHEMA_VALIDATION_ERROR", error_details
    except Exception as e:
        return False, None, "CORRUPT_JSON_SYNTAX", str(e)


def send_to_dead_letter_queue(
    dlq_producer: Producer,
    dlq_topic: str,
    original_topic: str,
    partition: int,
    offset: int,
    raw_payload: str,
    error_reason: str,
    error_details: Optional[str]
):
    """
    Wraps bad records into a structured DeadLetterEnvelope and publishes to the DLQ topic.
    """
    envelope = DeadLetterEnvelope(
        error_reason=error_reason,
        error_details=error_details,
        original_topic=original_topic,
        original_partition=partition,
        original_offset=offset,
        raw_payload=raw_payload
    )
    
    dlq_producer.produce(
        topic=dlq_topic,
        key=str(offset).encode("utf-8"),
        value=envelope.model_dump_json().encode("utf-8")
    )
    dlq_producer.poll(0)
    logger.warning(f"[DLQ Route] Part: {partition} | Off: {offset} | Reason: {error_reason} ({error_details})")


def run_consumer(
    input_topic: str = "ecommerce-events",
    dlq_topic: str = "ecommerce-dead-letter",
    group_id: str = "ecommerce-stream-group",
    bootstrap_servers: str = "localhost:9092"
):
    """
    Main consumer loop with PostgreSQL persistence.
    """
    global running

    # ── PostgreSQL pool ──────────────────────────────────────────
    db.init_pool(min_size=2, max_size=10)

    consumer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    }

    dlq_producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "ecommerce-dlq-producer",
        "acks": "1"
    }

    consumer = Consumer(consumer_conf)
    dlq_producer = Producer(dlq_producer_conf)
    metrics = ConsumerMetrics()

    consumer.subscribe([input_topic])
    logger.info(f"Connected to Kafka at '{bootstrap_servers}'")
    logger.info(f"Subscribed to topic '{input_topic}' (Group: '{group_id}')")
    logger.info(f"Dead-Letter Queue configured at topic '{dlq_topic}'")
    logger.info("Waiting for incoming events (Press Ctrl+C to stop)...\n")

    uncommitted_count = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            metrics.total_received += 1
            raw_payload = msg.value().decode("utf-8", errors="replace")
            partition = msg.partition()
            offset = msg.offset()

            # Validate and cleanse incoming event via Pydantic model
            is_valid, event, error_reason, error_details = validate_and_cleanse_event(raw_payload)

            if is_valid and event is not None:
                metrics.valid_count += 1
                if event.event_type == "RETURN":
                    metrics.return_count += 1
                elif event.event_type == "PURCHASE":
                    metrics.purchase_count += 1

                if event.is_guest:
                    metrics.guest_count += 1

                # ── Persist to PostgreSQL ────────────────────────
                try:
                    db.insert_transaction({
                        **event.model_dump(),
                        "kafka_topic":     input_topic,
                        "kafka_partition": partition,
                        "kafka_offset":    offset,
                    })
                except Exception as db_err:
                    logger.error(f"[DB] Failed to insert transaction: {db_err}")

                # Periodic progress logging
                if metrics.total_received % 100 == 0 or metrics.total_received == 1:
                    metrics.print_progress(
                        event.event_type,
                        event.invoice_no,
                        event.total_amount or 0.0
                    )
            else:
                metrics.dlq_count += 1

                # ── Route to Kafka DLQ topic ─────────────────────
                send_to_dead_letter_queue(
                    dlq_producer=dlq_producer,
                    dlq_topic=dlq_topic,
                    original_topic=input_topic,
                    partition=partition,
                    offset=offset,
                    raw_payload=raw_payload,
                    error_reason=error_reason or "UNKNOWN_ERROR",
                    error_details=error_details
                )

                # ── Dual-write: also persist DLQ record to Postgres
                envelope = DeadLetterEnvelope(
                    error_reason=error_reason or "UNKNOWN_ERROR",
                    error_details=error_details,
                    original_topic=input_topic,
                    original_partition=partition,
                    original_offset=offset,
                    raw_payload=raw_payload,
                )
                try:
                    db.insert_dead_letter(envelope.model_dump())
                except Exception as db_err:
                    logger.error(f"[DB] Failed to insert DLQ record: {db_err}")

            uncommitted_count += 1

            if uncommitted_count >= 100:
                consumer.commit(asynchronous=False)
                uncommitted_count = 0

    except Exception as e:
        if running:
            logger.error(f"Unexpected consumer error: {e}", exc_info=True)
    finally:
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass

        dlq_producer.flush(timeout=5.0)
        consumer.close()
        db.close_pool()

        elapsed = time.time() - metrics.start_time
        avg_rate = metrics.total_received / elapsed if elapsed > 0 else 0

        print("\n" + "="*60)
        logger.info("CONSUMER SESSION SUMMARY:")
        logger.info(f"   * Total Received:   {metrics.total_received:,}")
        logger.info(f"   * Valid Cleaned:    {metrics.valid_count:,}")
        logger.info(f"   * Purchases:        {metrics.purchase_count:,}")
        logger.info(f"   * Returns/Refunds:  {metrics.return_count:,}")
        logger.info(f"   * Guest Checkouts:  {metrics.guest_count:,}")
        logger.info(f"   * Routed to DLQ:    {metrics.dlq_count:,}")
        logger.info(f"   * Elapsed Time:     {elapsed:.2f}s")
        logger.info(f"   * Processing Speed: {avg_rate:.1f} events/sec")
        print("="*60 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Real-Time E-commerce Kafka Stream Consumer & Validator")
    parser.add_argument("--topic", type=str, default="ecommerce-events", help="Input Kafka topic (default: ecommerce-events)")
    parser.add_argument("--dlq-topic", type=str, default="ecommerce-dead-letter", help="Dead-Letter Queue topic (default: ecommerce-dead-letter)")
    parser.add_argument("--group-id", type=str, default="ecommerce-stream-group", help="Consumer group ID (default: ecommerce-stream-group)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker address (default: localhost:9092)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_consumer(
        input_topic=args.topic,
        dlq_topic=args.dlq_topic,
        group_id=args.group_id,
        bootstrap_servers=args.bootstrap_servers
    )
