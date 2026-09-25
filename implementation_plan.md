# Real-Time E-commerce Analytics Pipeline: Implementation Plan

An end-to-end streaming data pipeline using historical e-commerce data replayed as events through Apache Kafka, processed & validated in Python, stored in PostgreSQL, queried with SQL, and visualized in Power BI (Local & Cloud VM).

---

## User Review Required

> [!IMPORTANT]
> **Power BI Strategy**:
> - **Option 1 (Direct Power BI REST API Push)**: The Python Consumer or an exporter pushes real-time aggregated metrics directly to the Power BI Service (Cloud) via the Power BI Streaming Dataset REST API. This achieves sub-second dashboard updates in the browser without keeping Power BI Desktop open.
> - **Option 2 (Relational SQL Analytics via Power BI Desktop/Service)**: Connect Power BI to PostgreSQL to run rich SQL queries, dimensional modeling, and historical trend dashboards.
> - **Recommendation**: We will implement **PostgreSQL with SQL Analytics first**, and then demonstrate how real-time streaming connects to Power BI.

> [!NOTE]
> **Progression Rule**: All development will be completed and verified **locally first** (Kafka and Postgres in local Docker; Python apps running directly). Cloud VM deployment (Ubuntu Linux, SSH, remote Docker Compose) will happen in the final phases.

---

## Architecture Overview

```
[ Historical Dataset (CSV) / Synthetic Generator ]
                     │
                     ▼
          [ Python Event Producer ]
                     │
              (JSON Messages)
                     │
                     ▼
            [ Apache Kafka (KRaft) ]
             Topic: `ecommerce-events`
                     │
                     ▼
          [ Python Event Consumer ]
          ├── Deserializer (JSON)
          ├── Schema Validator & Type Casts
          ├── Data Cleanser (negative prices, missing IDs)
          └── Dead-Letter Queue (DLQ) for malformed events
                     │
            ┌────────┴──────────────────────────┐
     (Valid Events)                       (Real-time Aggs)
            ▼                                   ▼
    [ PostgreSQL DB ]                 [ Power BI Service API ]
    ├── Raw Transactions (Facts)       (Real-Time Cloud Dashboard)
    ├── Customers/Products (Dims)
    └── Aggregated Rollups
            │
            ▼
   [ SQL Analytics & Power BI ]
```

---

## Detailed Phase Breakdown

### Phase 0: Environment & Architecture Foundations (Current)
- Python virtual environment isolation (`.venv`).
- Architectural mental models (VM vs. Docker vs. Application).
- Kafka fundamentals: Broker, Topic, Partition, Offset, Consumer Group.
- Workspace initialization (`requirements.txt`, `.gitignore`, folder structure).

### Phase 1: Local Kafka with Docker
- Run Kafka in Docker using KRaft mode (no ZooKeeper dependency).
- Learn Docker commands: `docker compose up`, `docker ps`, `docker logs`, `docker exec`.
- Kafka CLI operations: Create topic `ecommerce-events`, inspect topic details, test console producer & consumer.

### Phase 2: Basic Python Kafka Producer
- Connect Python to Kafka using `confluent-kafka`.
- Implement basic message serialization (JSON to UTF-8 bytes).
- Understand delivery reports, callbacks, flushing, and message keys.
- Produce sample single messages.

### Phase 3: Basic Python Kafka Consumer
- Connect Python consumer to Kafka.
- Understand polling loop (`poll(timeout)`), consumer groups, and offset auto-commit vs. manual commit.
- Manually test consumer crash, restart, and offset resumption behavior.

### Phase 4: Historical Dataset Analysis & Schema Definition
- Download and inspect the UCI Online Retail / Online Retail II dataset.
- Data profiling with Pandas: analyze columns (`InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`).
- Identify dirty data: cancellations (`InvoiceNo` starting with 'C'), negative quantities, 0.0 unit prices, missing `CustomerID`.
- Define standardized JSON Event Schema.

### Phase 5: Historical Event Replayer (Producer)
- Implement `producer.py` to stream historical records row-by-row.
- Add configurable throughput control (`--events-per-second`).
- Implement graceful shutdown on `SIGINT` (Ctrl+C).

### Phase 6: Consumer Pipeline, Validation & Dead-Letter Queue (DLQ)
- Build robust validation logic in `consumer.py`.
- Handle schema errors, missing fields, out-of-range values.
- Route valid events to processing pipeline and bad events to a dedicated `ecommerce-dead-letter` Kafka topic.

### Phase 7: PostgreSQL Storage & Relational Modeling
- Start PostgreSQL via Docker Compose.
- Design database schema in `src/sql/schema.sql` (Tables: `transactions`, `products`, `customers`, `dead_letter_events`).
- Connect Python consumer to PostgreSQL via `psycopg2`.
- Batch / record insertion with transaction management.

### Phase 8: Idempotency & Delivery Semantics
- Understand At-Most-Once vs. At-Least-Once vs. Exactly-Once semantics.
- Handle duplicate event delivery using SQL `ON CONFLICT (event_id) DO NOTHING` or deduplication keys.
- Coordinate database commit with Kafka offset commit to prevent data duplication.

### Phase 9: Failure Injection & Resilience Testing
- Intentionally simulate and document:
  1. Consumer crash before offset commit.
  2. PostgreSQL down while Kafka is receiving events.
  3. Kafka broker restart.
  4. Malformed JSON payloads.
- Document exact failure recovery behaviors for interview readiness.

### Phase 10: Synthetic Event Generator
- Build `test_synthetic.py` to produce configurable test scenarios:
  - `NORMAL`, `TRAFFIC_SPIKE`, `MALFORMED_JSON`, `MISSING_FIELDS`, `INVALID_VALUES`, `DUPLICATES`, `BURST`.

### Phase 11: Kafka Partitioning & Horizontal Scaling
- Increase topic partition count from 1 to 3.
- Assign partition keys (e.g., `customer_id` or `country`).
- Run multiple consumer instances in the same consumer group and observe dynamic partition rebalancing.

### Phase 12: SQL Analytics
- Write structured analytical queries in `src/sql/analytics.sql`:
  - Total revenue & Average Order Value (AOV).
  - Hourly & daily sales velocity.
  - Top 10 products by volume and revenue.
  - Customer retention & repeat purchase rates.
  - Geo-distribution of orders.

### Phase 13: Power BI Dashboard (Desktop & Cloud Streaming)
- **Approach A**: Direct connection to PostgreSQL for analytical exploration.
- **Approach B**: Push Real-time Streaming API directly to Power BI Service in the cloud for live tile monitoring without desktop dependency.

### Phase 14: End-to-End Latency & Pipeline Metrics
- Track event timestamps across the pipeline: `created_at` $\rightarrow$ `kafka_received_at` $\rightarrow$ `db_inserted_at`.
- Calculate end-to-end latency and throughput metrics.

### Phase 15: Containerization & Cloud VM Deployment
- Containerize Python Producer and Consumer using multi-stage `Dockerfiles`.
- Provision a cloud Virtual Machine (e.g., Oracle Cloud Always Free, AWS EC2, or Azure VM).
- Configure Linux environment: SSH keys, firewall/security groups, Docker & Docker Compose installation.
- Deploy full multi-container stack via `docker-compose.prod.yml`.
- Write comprehensive `README.md` with system design, failure models, SQL benchmarks, and interview defense guide.

---

## Verification Plan

### Automated & Scripted Verification
- Unit test event validators (`pytest`).
- Synthetic generator runs to verify 0% data loss under failure scenarios.
- SQL verification queries confirming row counts, constraints, and aggregation correctness.

### Manual Verification
- Manual verification of Docker container lifecycle (`docker compose down / up`).
- Live consumer logs verification during producer streaming.
- Visual inspection of Power BI dashboard updates under live data feed.
