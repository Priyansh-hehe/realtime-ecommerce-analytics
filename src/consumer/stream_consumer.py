"""
src/consumer/stream_consumer.py
-------------------------------
Stream Consumer, Schema Validator, and Dead-Letter Queue (DLQ) Router.

Responsibilities:
1. Subscribes to `ecommerce-events` topic.
2. Deserializes JSON payloads and performs strict schema validation and type cleansing.
3. Classifies transaction types (PURCHASE, RETURN / CANCELLATION, GUEST_CHECKOUT).
4. Routes malformed, corrupt, or invalid business records to `ecommerce-dead-letter` topic.
5. Manages manual offset commits for reliable processing guarantees.
"""

import os
import sys
import json
import uuid
import time
import signal
import logging
import argparse
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Ensure clean UTF-8 console output for Windows terminal
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

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
            f"[{event_type:<8}] Inv: {invoice_no:<8} | "
            f"${amount:>7.2f}"
        )


def validate_and_cleanse_event(raw_payload: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Validates and cleanses raw incoming event JSON.

    Returns:
        (is_valid, cleansed_event_dict, error_reason)
    """
    # 1. JSON Parsing Check
    try:
        data = json.loads(raw_payload)
    except Exception as e:
        return False, None, f"CORRUPT_JSON_SYNTAX: {str(e)}"

    if not isinstance(data, dict):
        return False, None, "INVALID_ROOT_TYPE: Expected JSON object"

    # 2. Mandatory Fields Check
    required_fields = ["invoice_no", "stock_code", "quantity", "unit_price", "invoice_date"]
    missing = [field for field in required_fields if field not in data or data[field] is None or str(data[field]).strip() == ""]
    if missing:
        return False, data, f"MISSING_REQUIRED_FIELDS: {', '.join(missing)}"

    # 3. Numeric & Business Validation
    try:
        quantity = int(data["quantity"])
    except (ValueError, TypeError):
        return False, data, f"INVALID_QUANTITY_TYPE: '{data.get('quantity')}' is not a valid integer"

    try:
        unit_price = float(data["unit_price"])
    except (ValueError, TypeError):
        return False, data, f"INVALID_PRICE_TYPE: '{data.get('unit_price')}' is not a valid float"

    # Edge Case: Negative Unit Price is an invalid accounting error -> DLQ
    if unit_price < 0:
        return False, data, f"INVALID_NEGATIVE_PRICE: Unit price cannot be negative (${unit_price})"

    # 4. Cleanse & Standardize Fields
    invoice_no = str(data["invoice_no"]).strip()
    stock_code = str(data["stock_code"]).strip()
    description = str(data.get("description", "")).strip() or "NO DESCRIPTION"
    country = str(data.get("country", "Unknown")).strip()
    invoice_date = str(data["invoice_date"]).strip()

    # Customer ID Handling (Guest vs Registered)
    raw_cust = data.get("customer_id")
    customer_id = None
    if raw_cust is not None and str(raw_cust).strip().lower() not in ("nan", "none", "null", ""):
        try:
            customer_id = int(float(raw_cust))
        except (ValueError, TypeError):
            customer_id = str(raw_cust).strip()

    # Classify Event Type
    is_cancellation = invoice_no.upper().startswith("C") or quantity < 0
    if is_cancellation:
        event_type = "RETURN"
    elif unit_price == 0.0:
        event_type = "PROMO_FREE"
    else:
        event_type = "PURCHASE"

    total_amount = round(quantity * unit_price, 2)

    cleansed = {
        "event_id": data.get("event_id") or str(uuid.uuid4()),
        "invoice_no": invoice_no,
        "stock_code": stock_code,
        "description": description,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "invoice_date": invoice_date,
        "customer_id": customer_id,
        "country": country,
        "event_type": event_type,
        "is_cancellation": is_cancellation,
        "is_guest": customer_id is None,
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

    return True, cleansed, None


def send_to_dead_letter_queue(
    dlq_producer: Producer,
    dlq_topic: str,
    original_topic: str,
    partition: int,
    offset: int,
    raw_payload: str,
    error_reason: str
):
    """
    Wraps bad records with error context and publishes them to the DLQ topic.
    """
    dlq_envelope = {
        "dlq_id": str(uuid.uuid4()),
        "error_reason": error_reason,
        "original_topic": original_topic,
        "original_partition": partition,
        "original_offset": offset,
        "raw_payload": raw_payload,
        "rejected_at": datetime.now(timezone.utc).isoformat()
    }
    
    dlq_producer.produce(
        topic=dlq_topic,
        key=str(offset).encode("utf-8"),
        value=json.dumps(dlq_envelope).encode("utf-8")
    )
    dlq_producer.poll(0)
    logger.warning(f"⚠️ [DLQ Route] Part: {partition} | Off: {offset} | Reason: {error_reason}")


def run_consumer(
    input_topic: str = "ecommerce-events",
    dlq_topic: str = "ecommerce-dead-letter",
    group_id: str = "ecommerce-stream-group",
    bootstrap_servers: str = "localhost:9092"
):
    """
    Main consumer loop.
    """
    global running

    # Consumer Configuration
    consumer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,      # Manual commit for reliable processing
        "max.poll.interval.ms": 300000,
    }

    # DLQ Producer Configuration
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

            # Validate and cleanse incoming event
            is_valid, event, error_reason = validate_and_cleanse_event(raw_payload)

            if is_valid:
                metrics.valid_count += 1
                if event["event_type"] == "RETURN":
                    metrics.return_count += 1
                elif event["event_type"] == "PURCHASE":
                    metrics.purchase_count += 1
                
                if event["is_guest"]:
                    metrics.guest_count += 1

                # Periodic progress logging
                if metrics.total_received % 100 == 0 or metrics.total_received == 1:
                    metrics.print_progress(
                        event["event_type"],
                        event["invoice_no"],
                        event["total_amount"]
                    )
            else:
                # Malformed / Invalid event -> route to DLQ
                metrics.dlq_count += 1
                send_to_dead_letter_queue(
                    dlq_producer=dlq_producer,
                    dlq_topic=dlq_topic,
                    original_topic=input_topic,
                    partition=partition,
                    offset=offset,
                    raw_payload=raw_payload,
                    error_reason=error_reason
                )

            uncommitted_count += 1

            # Batch offset commit every 100 messages to balance throughput and safety
            if uncommitted_count >= 100:
                consumer.commit(asynchronous=False)
                uncommitted_count = 0

    except Exception as e:
        if running:
            logger.error(f"Unexpected consumer error: {e}", exc_info=True)
    finally:
        # Commit final offsets before exiting
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass

        # Flush any remaining DLQ messages
        dlq_producer.flush(timeout=5.0)
        consumer.close()

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
