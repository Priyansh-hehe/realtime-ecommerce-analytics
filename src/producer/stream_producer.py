"""
src/producer/stream_producer.py
-------------------------------
Historical E-commerce Event Replayer (Kafka Producer).

Streams real-world transaction events from `data/raw/online_retail.csv` into
Apache Kafka with configurable throughput, partition key routing, and delivery tracking.
"""

import os
import sys
import csv
import json
import uuid
import time
import signal
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError

# Ensure clean UTF-8 console output for Windows terminals
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("stream_producer")

# Global graceful shutdown flag
running = True


def handle_sigint(signum, frame):
    """Graceful shutdown on Ctrl+C."""
    global running
    print("\n")
    logger.info("🛑 Interrupt signal (Ctrl+C) received. Stopping event stream...")
    running = False


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


class StreamStats:
    """Tracks streaming metrics for progress reporting."""
    def __init__(self):
        self.sent_count = 0
        self.delivered_count = 0
        self.failed_count = 0
        self.start_time = time.time()

    def delivery_callback(self, err, msg):
        if err is not None:
            self.failed_count += 1
            logger.error(f"[ERROR] Delivery failed: {err}")
        else:
            self.delivered_count += 1

    def print_progress(self, current_invoice="", current_customer=""):
        elapsed = time.time() - self.start_time
        rate = self.sent_count / elapsed if elapsed > 0 else 0
        logger.info(
            f">> [Stream] Sent: {self.sent_count:>6,} | "
            f"Delivered: {self.delivered_count:>6,} | "
            f"Failed: {self.failed_count} | "
            f"Rate: {rate:>5.1f} evt/s | "
            f"Inv: {current_invoice:<8} | "
            f"Cust: {current_customer or 'Guest'}"
        )


def build_event(row: dict) -> dict:
    """
    Transforms a raw CSV row dictionary into a standardized JSON event.
    """
    invoice_no = str(row.get("invoice_no", "")).strip()
    stock_code = str(row.get("stock_code", "")).strip()
    description = str(row.get("description", "")).strip() or None
    
    # Safe numerical casting
    try:
        quantity = int(float(row.get("quantity", 0)))
    except (ValueError, TypeError):
        quantity = 0

    try:
        unit_price = float(row.get("unit_price", 0.0))
    except (ValueError, TypeError):
        unit_price = 0.0

    raw_cust_id = row.get("customer_id", "").strip()
    customer_id = None
    if raw_cust_id and raw_cust_id.lower() not in ("nan", "none", "null", ""):
        try:
            customer_id = int(float(raw_cust_id))
        except (ValueError, TypeError):
            customer_id = raw_cust_id

    country = str(row.get("country", "Unknown")).strip()
    invoice_date = row.get("invoice_date", "").strip()

    # Derived fields
    is_cancellation = invoice_no.upper().startswith("C") or quantity < 0
    total_amount = round(quantity * unit_price, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "invoice_no": invoice_no,
        "stock_code": stock_code,
        "description": description,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "invoice_date": invoice_date,
        "customer_id": customer_id,
        "country": country,
        "is_cancellation": is_cancellation,
        "produced_at": datetime.now(timezone.utc).isoformat()
    }


def stream_events(
    csv_file: str,
    topic: str,
    rate_limit: int,
    max_events: int,
    bootstrap_servers: str
):
    """
    Reads rows from the CSV and streams them into Kafka.
    """
    global running
    
    if not os.path.exists(csv_file):
        logger.error(f"❌ Dataset file not found: {csv_file}")
        logger.info("   Run `python src/producer/profiler.py` first to generate the combined dataset.")
        return

    # Kafka Producer Configuration
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "ecommerce-stream-producer",
        "acks": "1",
        "retries": 3,
        "retry.backoff.ms": 200,
        "linger.ms": 10,                 # Micro-batching buffer delay (10ms)
        "batch.num.messages": 1000,      # Buffer batch size
        "compression.type": "snappy"     # Efficient compression
    }

    logger.info(f"Connecting to Kafka at '{bootstrap_servers}'...")
    producer = Producer(conf)
    stats = StreamStats()

    logger.info(f"Starting event replay from '{csv_file}'")
    logger.info(f"Topic: '{topic}' | Target Rate: {rate_limit} events/sec | Max Events: {'All' if max_events <= 0 else f'{max_events:,}'}")

    # Delay per message to control throughput
    sleep_interval = 1.0 / rate_limit if rate_limit > 0 else 0

    with open(csv_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if not running:
                break

            event = build_event(row)
            
            # Partition Key Routing:
            # Use customer_id if present to keep customer journeys ordered.
            # Fallback to invoice_no for guest checkouts.
            partition_key = str(event["customer_id"]) if event["customer_id"] is not None else str(event["invoice_no"])
            key_bytes = partition_key.encode("utf-8")
            val_bytes = json.dumps(event).encode("utf-8")

            # Asynchronously send message
            producer.produce(
                topic=topic,
                key=key_bytes,
                value=val_bytes,
                callback=stats.delivery_callback
            )
            
            stats.sent_count += 1
            
            # Poll callbacks regularly without blocking
            producer.poll(0)

            # Periodic progress logging (every 100 events or every second)
            if stats.sent_count % 100 == 0:
                stats.print_progress(event["invoice_no"], str(event["customer_id"] or ""))

            # Stop if max_events reached
            if 0 < max_events <= stats.sent_count:
                logger.info(f"Reached maximum requested events limit ({max_events:,}).")
                break

            # Rate limiting
            if sleep_interval > 0:
                time.sleep(sleep_interval)

    # Final flush to ensure all queued messages are sent
    logger.info("Flushing remaining messages to Kafka broker...")
    remaining = producer.flush(timeout=15.0)

    elapsed = time.time() - stats.start_time
    avg_rate = stats.sent_count / elapsed if elapsed > 0 else 0
    
    print("\n" + "="*60)
    logger.info("STREAMING SUMMARY:")
    logger.info(f"   * Total Sent:       {stats.sent_count:,}")
    logger.info(f"   * Total Delivered:  {stats.delivered_count:,}")
    logger.info(f"   * Total Failed:     {stats.failed_count:,}")
    logger.info(f"   * Elapsed Time:     {elapsed:.2f}s")
    logger.info(f"   * Average Speed:    {avg_rate:.1f} events/sec")
    if remaining > 0:
        logger.warning(f"   * Unflushed Buffer: {remaining} messages")
    print("="*60 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Real-Time E-commerce Kafka Event Replayer")
    parser.add_argument(
        "--file",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "online_retail.csv"),
        help="Path to the combined online retail CSV file"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="ecommerce-events",
        help="Kafka topic to publish events to (default: ecommerce-events)"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Throughput in events per second (default: 100, set to 0 for unlimited)"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Max events to stream before stopping (0 for entire dataset, e.g. 500 for testing)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap server address (default: localhost:9092)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream_events(
        csv_file=args.file,
        topic=args.topic,
        rate_limit=args.rate,
        max_events=args.max_events,
        bootstrap_servers=args.bootstrap_servers
    )
