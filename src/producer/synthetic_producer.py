"""
src/producer/synthetic_producer.py
----------------------------------
Synthetic E-Commerce Event Generator & Chaos Simulator.

Simulates realistic and adversarial traffic scenarios to stress-test:
1. Pydantic validation & cleansing
2. Dead-Letter Queue (DLQ) routing
3. PostgreSQL idempotent writes & deduplication
4. Consumer throughput & latency under burst load

Scenarios:
- NORMAL:         Standard e-commerce purchase & return stream.
- SPIKE:          Sudden high-velocity traffic burst (Black Friday surge).
- DIRTY:          Payloads with business logic violations (negative price, empty invoice).
- MALFORMED:      Corrupt JSON payloads (broken syntax, truncated strings).
- DUPLICATES:     Duplicate event IDs to test database deduplication / idempotency.
- CHAOS:          Realistic production blend (85% valid, 5% return, 5% dirty, 3% corrupt, 2% dup).
"""

import os
import sys
import json
import time
import uuid
import random
import signal
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional

from confluent_kafka import Producer, KafkaError

# Ensure clean UTF-8 console output for Windows terminal
if sys.platform == "win32":
    getattr(sys.stdout, "reconfigure", lambda **kw: None)(encoding="utf-8")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("synthetic_producer")

# Graceful shutdown flag
running = True


def handle_sigint(signum, frame):
    """Graceful shutdown handler for Ctrl+C."""
    global running
    print("\n")
    logger.info("🛑 Interrupt signal (Ctrl+C) received. Stopping generator...")
    running = False


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


# Sample catalogs for synthetic generation
SAMPLE_PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.95),
    ("71053", "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.75),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART.", 3.75),
    ("22752", "SET 7 BABUSHKA NESTING BOXES", 7.65),
    ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ("22632", "HAND WARMER RED RETROSPOT", 1.85),
    ("22633", "HAND WARMER UNION JACK", 1.85),
    ("22865", "HAND WARMER OWL DESIGN", 1.85),
    ("22634", "CHILDS BREAKFAST SET SPACEBOY", 9.95),
    ("20725", "LUNCH BAG RED RETROSPOT", 1.65),
    ("20727", "LUNCH BAG BLACK SKULL.", 1.65),
    ("20728", "LUNCH BAG CARS BLUE", 1.65),
    ("22382", "LUNCH BAG SPACEBOY DESIGN", 1.65),
    ("22383", "LUNCH BAG SUKI DESIGN", 1.65),
    ("POST", "POSTAGE", 18.00),
    ("M", "Manual", 1.25),
    ("22423", "REGENCY CAKESTAND 3 TIER", 12.75),
    ("47566", "PARTY BUNTING", 4.95),
]

COUNTRIES = [
    ("United Kingdom", 0.80),
    ("Germany", 0.05),
    ("France", 0.05),
    ("EIRE", 0.03),
    ("Spain", 0.02),
    ("Netherlands", 0.02),
    ("Australia", 0.01),
    ("United States", 0.01),
    ("Switzerland", 0.01),
]

CUSTOMER_IDS = [12345 + i for i in range(200)]  # 200 regular customers


def choose_country() -> str:
    """Weighted random selection of country."""
    names, weights = zip(*COUNTRIES)
    return random.choices(names, weights=weights, k=1)[0]


def generate_valid_event(custom_event_id: Optional[str] = None) -> Dict[str, Any]:
    """Generates a realistic valid e-commerce transaction."""
    stock_code, description, base_price = random.choice(SAMPLE_PRODUCTS)
    
    # 85% purchases, 15% returns
    is_return = random.random() < 0.15
    invoice_prefix = "C" if is_return else ""
    invoice_num = f"{invoice_prefix}{random.randint(536365, 581587)}"
    
    quantity = -random.randint(1, 10) if is_return else random.randint(1, 24)
    unit_price = round(max(0.1, base_price * random.uniform(0.85, 1.15)), 2)

    # 25% guest checkouts (customer_id = None)
    customer_id = None if random.random() < 0.25 else random.choice(CUSTOMER_IDS)

    return {
        "event_id": custom_event_id or str(uuid.uuid4()),
        "invoice_no": invoice_num,
        "stock_code": stock_code,
        "description": description,
        "quantity": quantity,
        "unit_price": unit_price,
        "invoice_date": datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M"),
        "customer_id": customer_id,
        "country": choose_country(),
    }


def generate_dirty_event() -> Dict[str, Any]:
    """Generates an event that violates business validation rules."""
    defect_type = random.choice(["negative_price", "empty_invoice", "empty_stock", "corrupt_customer"])
    event = generate_valid_event()

    if defect_type == "negative_price":
        event["unit_price"] = -random.uniform(5.0, 50.0)  # Fails ge=0.0 check
    elif defect_type == "empty_invoice":
        event["invoice_no"] = "   "                       # Fails min_length=1 clean_strings check
    elif defect_type == "empty_stock":
        event["stock_code"] = ""                          # Fails min_length=1
    elif defect_type == "corrupt_customer":
        event["customer_id"] = "not-a-number-customer"    # Will be converted to None or error
        event["unit_price"] = -1.0                        # Combined error

    return event


def generate_malformed_json() -> str:
    """Generates syntactically broken JSON strings."""
    malformations = [
        '{"invoice_no": "536365", "quantity": 12, "unit_price": ',                    # Truncated JSON
        '{invoice_no: 536365, unquoted_keys: true}',                                   # Missing quotes
        '<<<CORRUPT_BYTES_DATA_STREAM>>>',                                             # Pure garbage
        '{"event_id": null, "broken_array": [1, 2, 3',                                # Unclosed bracket
    ]
    return random.choice(malformations)


def generate_event_by_scenario(scenario: str, dupe_pool: list) -> Tuple[str, str]:
    """
    Returns (payload_str, category_label).
    """
    if scenario == "NORMAL":
        return json.dumps(generate_valid_event()), "VALID"

    elif scenario == "DIRTY":
        return json.dumps(generate_dirty_event()), "DIRTY"

    elif scenario == "MALFORMED":
        return generate_malformed_json(), "MALFORMED"

    elif scenario == "DUPLICATES":
        if dupe_pool and random.random() < 0.6:
            # Replay a previously generated event ID
            replayed_id = random.choice(dupe_pool)
            return json.dumps(generate_valid_event(custom_event_id=replayed_id)), "DUPLICATE"
        else:
            event = generate_valid_event()
            dupe_pool.append(event["event_id"])
            if len(dupe_pool) > 50:
                dupe_pool.pop(0)
            return json.dumps(event), "VALID"

    elif scenario == "CHAOS":
        roll = random.random()
        if roll < 0.80:
            return json.dumps(generate_valid_event()), "VALID"
        elif roll < 0.90:
            return json.dumps(generate_dirty_event()), "DIRTY"
        elif roll < 0.95:
            return generate_malformed_json(), "MALFORMED"
        else:
            if dupe_pool:
                replayed_id = random.choice(dupe_pool)
                return json.dumps(generate_valid_event(custom_event_id=replayed_id)), "DUPLICATE"
            else:
                event = generate_valid_event()
                dupe_pool.append(event["event_id"])
                return json.dumps(event), "VALID"

    # Default fallback
    return json.dumps(generate_valid_event()), "VALID"


def run_generator(
    scenario: str = "CHAOS",
    topic: str = "ecommerce-events",
    rate_per_sec: float = 20.0,
    max_events: int = 1000,
    bootstrap_servers: str = "localhost:9092"
):
    """
    Main generator loop.
    """
    global running

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "synthetic-event-generator",
        "linger.ms": 5,
        "acks": "1"
    }

    producer = Producer(producer_conf)
    logger.info(f"Synthetic Generator started on topic '{topic}'")
    logger.info(f"Scenario: [{scenario.upper()}] | Target Rate: {rate_per_sec}/sec | Max Events: {max_events}")

    counts = {"VALID": 0, "DIRTY": 0, "MALFORMED": 0, "DUPLICATE": 0}
    dupe_pool = []
    start_time = time.time()
    events_sent = 0

    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0

    try:
        while running and (max_events == 0 or events_sent < max_events):
            loop_start = time.time()

            payload_str, category = generate_event_by_scenario(scenario.upper(), dupe_pool)
            
            # Send message to Kafka
            producer.produce(
                topic=topic,
                key=str(random.randint(1, 100)).encode("utf-8"),
                value=payload_str.encode("utf-8")
            )
            producer.poll(0)

            events_sent += 1
            counts[category] = counts.get(category, 0) + 1

            if events_sent % 50 == 0 or events_sent == 1:
                elapsed = time.time() - start_time
                curr_rate = events_sent / elapsed if elapsed > 0 else 0
                logger.info(
                    f">> [Synthetic] Sent: {events_sent:>5,} | "
                    f"Rate: {curr_rate:>5.1f}/s | "
                    f"Valid: {counts['VALID']} | "
                    f"Dirty: {counts['DIRTY']} | "
                    f"Corrupt: {counts['MALFORMED']} | "
                    f"Dupes: {counts['DUPLICATE']}"
                )

            # Precise rate limiting
            if interval > 0:
                spent = time.time() - loop_start
                sleep_time = interval - spent
                if sleep_time > 0:
                    time.sleep(sleep_time)

    except Exception as e:
        if running:
            logger.error(f"Generator error: {e}", exc_info=True)
    finally:
        producer.flush(timeout=5.0)
        elapsed = time.time() - start_time
        avg_rate = events_sent / elapsed if elapsed > 0 else 0

        print("\n" + "="*60)
        logger.info("SYNTHETIC GENERATOR SUMMARY:")
        logger.info(f"   * Scenario:         {scenario.upper()}")
        logger.info(f"   * Total Sent:       {events_sent:,}")
        logger.info(f"   * Valid Generated:  {counts.get('VALID', 0):,}")
        logger.info(f"   * Dirty / Invalid:  {counts.get('DIRTY', 0):,}")
        logger.info(f"   * Malformed JSON:   {counts.get('MALFORMED', 0):,}")
        logger.info(f"   * Duplicates Sent:  {counts.get('DUPLICATE', 0):,}")
        logger.info(f"   * Elapsed Time:     {elapsed:.2f}s")
        logger.info(f"   * Average Speed:    {avg_rate:.1f} events/sec")
        print("="*60 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Synthetic E-Commerce Event Generator & Chaos Simulator")
    parser.add_argument(
        "--scenario",
        type=str,
        default="CHAOS",
        choices=["NORMAL", "DIRTY", "MALFORMED", "DUPLICATES", "CHAOS"],
        help="Traffic scenario to generate (default: CHAOS)"
    )
    parser.add_argument("--rate", type=float, default=25.0, help="Target events per second (default: 25.0)")
    parser.add_argument("--max-events", type=int, default=500, help="Maximum events to generate (0 for infinite, default: 500)")
    parser.add_argument("--topic", type=str, default="ecommerce-events", help="Target Kafka topic (default: ecommerce-events)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker address (default: localhost:9092)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_generator(
        scenario=args.scenario,
        topic=args.topic,
        rate_per_sec=args.rate,
        max_events=args.max_events,
        bootstrap_servers=args.bootstrap_servers
    )
