-- =============================================================
-- src/sql/schema.sql
-- E-Commerce Analytics — PostgreSQL 16 Schema
--
-- This file is auto-executed by the Postgres container on first
-- startup (mounted at /docker-entrypoint-initdb.d/schema.sql).
-- Re-running it is safe thanks to IF NOT EXISTS guards.
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- 0. Extensions
-- ─────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- gen_random_uuid()


-- ─────────────────────────────────────────────────────────────
-- 1. ENUM types
-- ─────────────────────────────────────────────────────────────
DO $$ BEGIN
    CREATE TYPE event_type_enum AS ENUM ('PURCHASE', 'RETURN', 'PROMO_FREE');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ─────────────────────────────────────────────────────────────
-- 2. transactions  (one row per Kafka event)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    -- Internal surrogate key
    id              BIGSERIAL       PRIMARY KEY,

    -- Business identifiers (from Pydantic-validated payload)
    event_id        UUID            NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    invoice_no      VARCHAR(20)     NOT NULL,
    stock_code      VARCHAR(20)     NOT NULL,
    description     TEXT,
    quantity        INTEGER         NOT NULL,
    unit_price      NUMERIC(12, 4)  NOT NULL CHECK (unit_price >= 0),
    total_amount    NUMERIC(14, 2)  NOT NULL,
    invoice_date    TEXT            NOT NULL,   -- kept as string from source
    customer_id     INTEGER,                    -- NULL = guest checkout
    country         VARCHAR(100)    NOT NULL DEFAULT 'Unknown',

    -- Pipeline-derived classifications
    event_type      event_type_enum NOT NULL DEFAULT 'PURCHASE',
    is_cancellation BOOLEAN         NOT NULL DEFAULT FALSE,
    is_guest        BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Kafka provenance (for traceability / replay)
    kafka_topic     VARCHAR(100),
    kafka_partition INTEGER,
    kafka_offset    BIGINT,

    -- Timestamps & End-to-End Latency Tracking
    produced_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    inserted_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    latency_ms      NUMERIC(10, 2)  -- End-to-end latency: (inserted_at - produced_at) in milliseconds
);

-- Indexes for common analytics query patterns
CREATE INDEX IF NOT EXISTS idx_txn_invoice_no    ON transactions (invoice_no);
CREATE INDEX IF NOT EXISTS idx_txn_customer_id   ON transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_event_type    ON transactions (event_type);
CREATE INDEX IF NOT EXISTS idx_txn_country       ON transactions (country);
CREATE INDEX IF NOT EXISTS idx_txn_processed_at  ON transactions (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_txn_latency_ms    ON transactions (latency_ms);


-- ─────────────────────────────────────────────────────────────
-- 3. dead_letter_queue  (audit log of rejected / malformed events)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id               BIGSERIAL   PRIMARY KEY,
    dlq_id           UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    error_reason     VARCHAR(60) NOT NULL,    -- e.g. SCHEMA_VALIDATION_ERROR
    error_details    TEXT,                    -- field-level error messages
    original_topic   VARCHAR(100),
    original_partition INTEGER,
    original_offset  BIGINT,
    raw_payload      TEXT        NOT NULL,
    rejected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_reason  ON dead_letter_queue (error_reason);
CREATE INDEX IF NOT EXISTS idx_dlq_rejected_at   ON dead_letter_queue (rejected_at DESC);


-- ─────────────────────────────────────────────────────────────
-- 4. Useful views
-- ─────────────────────────────────────────────────────────────

-- Revenue summary by country and event type
CREATE OR REPLACE VIEW v_revenue_by_country AS
SELECT
    country,
    event_type,
    COUNT(*)                AS transaction_count,
    SUM(total_amount)       AS total_revenue,
    AVG(total_amount)       AS avg_order_value,
    MIN(processed_at)       AS first_seen,
    MAX(processed_at)       AS last_seen
FROM transactions
GROUP BY country, event_type;


-- Top 20 products by quantity sold
CREATE OR REPLACE VIEW v_top_products AS
SELECT
    stock_code,
    MAX(description)        AS description,
    SUM(quantity)           AS total_units_sold,
    SUM(total_amount)       AS total_revenue,
    COUNT(DISTINCT invoice_no) AS num_invoices
FROM transactions
WHERE event_type = 'PURCHASE'
GROUP BY stock_code
ORDER BY total_units_sold DESC
LIMIT 20;


-- DLQ error rate summary
CREATE OR REPLACE VIEW v_dlq_summary AS
SELECT
    error_reason,
    COUNT(*) AS occurrences,
    MAX(rejected_at) AS last_seen
FROM dead_letter_queue
GROUP BY error_reason
ORDER BY occurrences DESC;


-- End-to-End Streaming Pipeline Latency & Health View
CREATE OR REPLACE VIEW v_pipeline_latency_metrics AS
SELECT
    COUNT(*)                                              AS total_events_processed,
    ROUND(AVG(latency_ms), 2)                             AS avg_latency_ms,
    ROUND(MIN(latency_ms), 2)                             AS min_latency_ms,
    ROUND(MAX(latency_ms), 2)                             AS max_latency_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p50_latency_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p95_latency_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p99_latency_ms
FROM transactions
WHERE latency_ms IS NOT NULL;
