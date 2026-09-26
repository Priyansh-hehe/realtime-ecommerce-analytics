"""
src/consumer/basic_consumer.py
------------------------------
Demonstrates a foundational Apache Kafka Consumer using the `confluent-kafka` library.

Key responsibilities:
1. Connects to Kafka broker (`localhost:9092`) as part of a named Consumer Group.
2. Subscribes to the `ecommerce-events` topic.
3. Runs a continuous polling loop (`consumer.poll()`) to retrieve incoming records.
4. Deserializes raw UTF-8 byte payloads into structured Python dictionaries.
5. Handles message errors, timeouts, and partition metadata inspection.
6. Implements graceful shutdown (`KeyboardInterrupt`) to cleanly commit offsets and close consumer.
"""

import json
import logging
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

# Configure clean console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


def create_consumer(
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "ecommerce-basic-group"
) -> Consumer:
    """
    Initializes and returns a Confluent Kafka Consumer instance.
    """
    conf = {
        # Address of the Kafka broker(s)
        "bootstrap.servers": bootstrap_servers,
        
        # Unique consumer group identifier
        "group.id": group_id,
        
        # Where to start reading if no committed offset exists for this group:
        # 'earliest': Start from the beginning of the topic (Offset 0)
        # 'latest': Start only from newly arrived messages
        "auto.offset.reset": "earliest",
        
        # Automatically commit consumed offsets in background every 5 seconds
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    return Consumer(conf)


def main():
    topic_name = "ecommerce-events"
    consumer = create_consumer()

    # Subscribe to the topic
    consumer.subscribe([topic_name])
    logger.info(f"Subscribed to topic '{topic_name}'. Waiting for events (Press Ctrl+C to stop)...")

    try:
        while True:
            # Poll for messages with a 1.0 second timeout
            msg = consumer.poll(timeout=1.0)

            # 1. No message arrived during this poll interval
            if msg is None:
                continue

            # 2. Check for Kafka-level errors (e.g. partition end, connection issue)
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()} at offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
                continue

            # 3. Extract metadata
            key = msg.key().decode("utf-8") if msg.key() else "None"
            raw_value = msg.value().decode("utf-8")
            partition = msg.partition()
            offset = msg.offset()

            # 4. Deserialize JSON payload into Python dictionary
            try:
                event_data = json.loads(raw_value)
                logger.info(
                    f"📥 [Part {partition} | Off {offset}] "
                    f"Customer: {key} | "
                    f"Invoice: {event_data.get('invoice_no')} | "
                    f"Product: {event_data.get('description')} | "
                    f"Qty: {event_data.get('quantity')} | "
                    f"Total: ${float(event_data.get('unit_price', 0)) * int(event_data.get('quantity', 0)):.2f}"
                )
            except json.JSONDecodeError as err:
                logger.error(f"❌ Failed to parse JSON at partition {partition}, offset {offset}: {err}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Stop signal received. Shutting down consumer...")
    finally:
        # Close consumer down to commit final offsets and leave consumer group cleanly
        consumer.close()
        logger.info("Consumer closed cleanly.")


if __name__ == "__main__":
    main()
