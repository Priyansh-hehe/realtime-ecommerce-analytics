# Real-Time E-Commerce Analytics — Command Reference Manual

A complete reference guide containing all CLI, Docker, Kafka, PostgreSQL, Python, and deployment commands used across this project.

---

## ⚡ Quick Start: Resume After Laptop Restart (Multi-Terminal Guide)

Whenever you restart your laptop or open the project, follow this exact sequence:

### 🖥️ Terminal 1 — Infrastructure (Start Containers)
```powershell
# 1. Start Kafka and PostgreSQL containers in the background
docker compose up -d

# 2. Verify both containers are running (Up / healthy)
docker ps
```

---

### 🖥️ Terminal 2 — Stream Consumer (Processing Engine)
```powershell
# 1. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 2. Start the consumer worker (leave this terminal open & running!)
python src/consumer/stream_consumer.py
```

---

### 🖥️ Terminal 3 — Producer (Generate Real-Time Events)
```powershell
# 1. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 2. Generate traffic (choose one):
# A) Normal clean traffic (200 events at 30/sec)
python src/producer/synthetic_producer.py --scenario NORMAL --rate 30 --max-events 200

# B) Chaos mode (includes malformed events & bad records to test DLQ)
python src/producer/synthetic_producer.py --scenario CHAOS --rate 35 --max-events 300

# C) Continuous infinite stream (runs until Ctrl+C)
python src/producer/synthetic_producer.py --scenario CHAOS --rate 30 --max-events 0
```

---

### 🖥️ Terminal 4 — Quick Verification & Analytics Inspection
```powershell
# Check total processed events and end-to-end latency metrics
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT * FROM v_pipeline_latency_metrics;"
```

---

## 1. Environment & Python Setup

### Activate Virtual Environment (PowerShell)
```powershell
# Activate the existing virtual environment
.\.venv\Scripts\Activate.ps1

# Verify active Python version
python --version

# Install / update all dependencies
pip install -r requirements.txt
```

### Recreate / Fix Virtual Environment (Windows Python 3.12)
```powershell
# If Python path is broken or upgraded:
Remove-Item -Recurse -Force .venv
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 2. Docker Infrastructure (Local Development)

### Start Local Kafka & PostgreSQL Containers
```powershell
# Start services in the background
docker compose up -d

# Check status of running containers
docker ps

# View container logs in real time
docker compose logs -f

# View logs for a specific service
docker compose logs -f postgres
docker compose logs -f kafka

# Stop containers (preserves database data volume)
docker compose down

# Stop containers and wipe database volumes (clean reset)
docker compose down -v
```

---

## 3. Data Profiling & Producers (Event Ingestion)

### Offline Dataset Profiling
```powershell
# Analyze dataset dimensions, null values, negative values, and anomalies
python src/producer/profiler.py
```

### Historical CSV Replay Producer
```powershell
# Stream CSV dataset with default settings
python src/producer/csv_replay_producer.py

# Stream at custom rate with delay
python src/producer/csv_replay_producer.py --rate 25 --delay-ms 40 --max-events 500
```

### Synthetic Chaos & Traffic Generator Producer
```powershell
# Normal traffic generation (30 events/sec, 200 events)
python src/producer/synthetic_producer.py --scenario NORMAL --rate 30 --max-events 200

# High-velocity flash sale simulation (50 events/sec)
python src/producer/synthetic_producer.py --scenario SPIKE --rate 50 --max-events 500

# Chaos mode (injects corrupt JSON, missing fields, schema violations for DLQ testing)
python src/producer/synthetic_producer.py --scenario CHAOS --rate 35 --max-events 300

# Continuous streaming (runs indefinitely until stopped with Ctrl+C)
python src/producer/synthetic_producer.py --scenario CHAOS --rate 30 --max-events 0
```

---

## 4. Stream Consumer & Worker Scaling

### Run Single Stream Consumer
```powershell
# Start consumer (listens to ecommerce-events, validates via Pydantic, writes to Postgres)
python src/consumer/stream_consumer.py
```

### Demonstrate Horizontal Scaling & Partition Rebalancing
Open 3 separate terminal windows with the virtual environment activated:
```powershell
# Terminal 1: Consumer Worker 1
python src/consumer/stream_consumer.py

# Terminal 2: Consumer Worker 2 (triggers Kafka rebalance: Partitions shared)
python src/consumer/stream_consumer.py

# Terminal 3: Consumer Worker 3 (each worker owns exactly 1 partition)
python src/consumer/stream_consumer.py
```

---

## 5. PostgreSQL Database Inspection & Metrics

### Interactive psql Shell
```powershell
docker exec -it postgres-ecommerce psql -U ecom_user -d ecommerce
```

### Single Command Health & Verification Checks
```powershell
# Check total transaction and DLQ counts
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT count(*) AS valid_transactions FROM transactions; SELECT count(*) AS dlq_records FROM dead_letter_queue;"

# View end-to-end pipeline latency metrics (avg, min, max, p50, p95, p99)
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT * FROM v_pipeline_latency_metrics;"

# Inspect Dead-Letter Queue error breakdown
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT * FROM v_dlq_summary;"

# View top 10 products by revenue and quantity
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT * FROM v_top_products LIMIT 10;"

# View revenue summary by country
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT * FROM v_revenue_by_country ORDER BY total_revenue DESC LIMIT 10;"

# Inspect most recent 5 transactions with latency
docker exec -i postgres-ecommerce psql -U ecom_user -d ecommerce -c "SELECT event_id, invoice_no, country, total_amount, event_type, latency_ms FROM transactions ORDER BY id DESC LIMIT 5;"
```

---

## 6. Kafka Broker Management (Inside Container CLI)

```powershell
# List all active topics
docker exec -it kafka-ecommerce /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Inspect partition details for the main topic
docker exec -it kafka-ecommerce /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ecommerce-events

# Check Consumer Group lag and partition assignment
docker exec -it kafka-ecommerce /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ecommerce-stream-group

# Read live raw messages from topic console consumer
docker exec -it kafka-ecommerce /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ecommerce-events --from-beginning --max-messages 5

# Read live Dead-Letter Queue rejected messages
docker exec -it kafka-ecommerce /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ecommerce-dead-letter --from-beginning --max-messages 5
```

---

## 7. Production Multi-Container Stack (`docker-compose.prod.yml`)

### Build and Launch Full Production Cluster
```powershell
# Starts Kafka (KRaft), Topic Initializer, PostgreSQL, 3 Parallel Consumers, and Traffic Producer
docker compose -f docker-compose.prod.yml up -d --build

# View container status of all production services
docker compose -f docker-compose.prod.yml ps

# Follow logs across all 3 consumer workers in parallel
docker compose -f docker-compose.prod.yml logs -f consumer-1 consumer-2 consumer-3

# Follow producer traffic generator logs
docker compose -f docker-compose.prod.yml logs -f producer

# Stop the entire production stack
docker compose -f docker-compose.prod.yml down
```

---

## 8. Power BI Desktop DirectQuery Connection

### Direct Local Connection:
1. Open **Power BI Desktop**.
2. **Get Data** → **PostgreSQL database**.
3. **Server:** `localhost:5432`
4. **Database:** `ecommerce`
5. **Data Connectivity Mode:** `DirectQuery`
6. **Username:** `ecom_user` | **Password:** `ecom_pass` (or as set in `.env`)
7. Select tables & views:
   - `transactions`
   - `v_pipeline_latency_metrics`
   - `v_revenue_by_country`
   - `v_top_products`
   - `v_dlq_summary`
8. In Canvas: Turn on **Page Refresh** → set to **1–5 seconds** for live streaming visuals.

---

## 9. OCI Remote Cloud Deployment & SSH Tunneling

### SSH into OCI Linux VM
```powershell
ssh -i "path\to\oci_private_key.key" ubuntu@<YOUR_OCI_PUBLIC_IP>
```

### Setup & Run on VM
```bash
# On remote VM:
git clone https://github.com/<YOUR_USERNAME>/realtime-ecommerce-analytics.git
cd realtime-ecommerce-analytics
cp .env.example .env
nano .env # Set your custom secure cloud password
docker compose -f docker-compose.prod.yml up -d --build
```

### Secure SSH Tunnel from Local Laptop (For Power BI DirectQuery)
```powershell
# In a local PowerShell window:
ssh -i "path\to\oci_private_key.key" -N -L 5432:localhost:5432 ubuntu@<YOUR_OCI_PUBLIC_IP>
```
*(Now Power BI Desktop connects to `localhost:5432` directly through the encrypted SSH tunnel to the cloud VM).*
