"""
src/producer/basic_producer.py
------------------------------
Demonstrates a foundational Apache Kafka Producer using the `confluent-kafka` library.

Key responsibilities:
1. Configures connection to local Kafka broker (`localhost:9092`).
2. Serializes Python dictionaries into JSON formatted UTF-8 byte arrays.
3. Implements an asynchronous delivery callback to confirm broker acknowledgement.
4. Demonstrates partition routing using message keys vs round-robin.
5. Flushes memory buffers to ensure zero message loss on exit.
"""

import json
import logging
import time
from typing import Any, Dict
from confluent_kafka import Producer

# Configure clean console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


def delivery_callback(err, msg):
    """
    Triggered once for each message to confirm delivery or handle delivery failures.
    
    Args:
        err: KafkaError object (if failure occurred), or None if successful.
        msg: Message object containing partition, offset, key, and timestamp.
    """
    if err is not None:
        logger.error(f"❌ Message delivery failed: {err}")
    else:
        logger.info(
            f"✅ Delivered to topic: '{msg.topic()}' | "
            f"Partition: [{msg.partition()}] | "
            f"Offset: {msg.offset()} | "
            f"Key: {msg.key().decode('utf-8') if msg.key() else 'None'}"
        )


def create_producer(bootstrap_servers: str = "localhost:9092") -> Producer:
    """
    Initializes and returns a Confluent Kafka Producer instance.
    """
    conf = {
        # Address of the Kafka broker(s)
        "bootstrap.servers": bootstrap_servers,
        
        # Client identifier for logging and broker diagnostics
        "client.id": "ecommerce-basic-producer",
        
        # Acknowledgment level:
        # 'all' (-1): Broker waits for all in-sync replicas to acknowledge before confirming.
        # '1': Leader broker writes to its local log and immediately confirms (balanced).
        "acks": "1",
        
        # Retry parameters for transient network hiccups
        "retries": 3,
        "retry.backoff.ms": 200,
    }
    return Producer(conf)


def main():
    topic_name = "ecommerce-events"
    producer = create_producer()

    logger.info(f"Connected to Kafka broker. Publishing sample events to '{topic_name}'...")

    # Sample e-commerce transactions
    sample_events = [
        {
            "invoice_no": "536365",
            "stock_code": "85123A",
            "description": "WHITE HANGING HEART T-LIGHT HOLDER",
            "quantity": 6,
            "unit_price": 2.55,
            "customer_id": "17850",
            "country": "United Kingdom",
            "timestamp": int(time.time()),
        },
        {
            "invoice_no": "536366",
            "stock_code": "22633",
            "description": "HAND WARMER UNION JACK",
            "quantity": 12,
            "unit_price": 1.85,
            "customer_id": "17850",  # Same customer -> should hash to same partition!
            "country": "United Kingdom",
            "timestamp": int(time.time()),
        },
        {
            "invoice_no": "536367",
            "stock_code": "84879",
            "description": "ASSORTED COLOUR BIRD ORNAMENT",
            "quantity": 32,
            "unit_price": 1.69,
            "customer_id": "13047",  # Different customer -> may land on different partition
            "country": "United Kingdom",
            "timestamp": int(time.time()),
        },
    ]

    for event in sample_events:
        # 1. Message Key: Using customer_id ensures all events for this customer
        # maintain strict sequential ordering within the same partition.
        key = str(event["customer_id"]).encode("utf-8")

        # 2. Value: Serialize Python dictionary to JSON string, then to UTF-8 bytes
        value = json.dumps(event).encode("utf-8")

        # 3. Asynchronously produce message to the local memory buffer
        producer.produce(
            topic=topic_name,
            key=key,
            value=value,
            callback=delivery_callback
        )
        
        # Serve delivery callback events from previous produce calls
        producer.poll(0)

    # 4. Flush: Wait for all outstanding buffered messages to be delivered
    logger.info("Flushing producer buffer to Kafka broker...")
    remaining = producer.flush(timeout=10.0)
    
    if remaining > 0:
        logger.warning(f"⚠️ {remaining} messages could not be delivered before timeout!")
    else:
        logger.info("🎉 All messages successfully published and acknowledged by Kafka!")


if __name__ == "__main__":
    main()
