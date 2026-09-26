-- =============================================================
-- src/sql/analytics.sql
-- Real-Time E-Commerce Analytics Queries
-- =============================================================

-- 1. Executive KPI Summary (Revenue, Orders, AOV, Returns)
SELECT
    COUNT(*) FILTER (WHERE event_type = 'PURCHASE') AS total_purchases,
    COUNT(*) FILTER (WHERE event_type = 'RETURN')   AS total_returns,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_type = 'RETURN') / NULLIF(COUNT(*), 0),
        2
    ) AS return_rate_pct,
    COALESCE(SUM(total_amount) FILTER (WHERE event_type = 'PURCHASE'), 0) AS gross_revenue,
    COALESCE(SUM(total_amount) FILTER (WHERE event_type = 'RETURN'), 0)   AS refunded_amount,
    COALESCE(SUM(total_amount), 0)                                       AS net_revenue,
    ROUND(
        AVG(total_amount) FILTER (WHERE event_type = 'PURCHASE'),
        2
    ) AS avg_order_value
FROM transactions;


-- 2. Revenue and Volume Breakdown by Country
SELECT
    country,
    COUNT(*)                                              AS transaction_count,
    COUNT(DISTINCT customer_id)                           AS unique_customers,
    COUNT(*) FILTER (WHERE is_guest = TRUE)               AS guest_checkouts,
    COALESCE(SUM(total_amount), 0)                        AS net_revenue,
    ROUND(AVG(total_amount), 2)                           AS avg_order_value,
    ROUND(
        100.0 * COALESCE(SUM(total_amount), 0) / NULLIF(SUM(SUM(total_amount)) OVER(), 0),
        2
    ) AS revenue_share_pct
FROM transactions
GROUP BY country
ORDER BY net_revenue DESC
LIMIT 10;


-- 3. Top 15 Best-Selling Products by Revenue
SELECT
    stock_code,
    description,
    SUM(quantity)                                        AS total_units_sold,
    SUM(total_amount)                                    AS total_revenue,
    COUNT(DISTINCT invoice_no)                           AS total_orders,
    ROUND(AVG(unit_price), 2)                            AS avg_unit_price
FROM transactions
WHERE event_type = 'PURCHASE'
GROUP BY stock_code, description
ORDER BY total_revenue DESC
LIMIT 15;


-- 4. Products with Highest Return Rates
SELECT
    stock_code,
    description,
    SUM(quantity) FILTER (WHERE event_type = 'PURCHASE') AS units_purchased,
    ABS(SUM(quantity) FILTER (WHERE event_type = 'RETURN')) AS units_returned,
    ROUND(
        100.0 * ABS(COALESCE(SUM(quantity) FILTER (WHERE event_type = 'RETURN'), 0)) /
        NULLIF(SUM(quantity) FILTER (WHERE event_type = 'PURCHASE'), 0),
        2
    ) AS return_volume_pct
FROM transactions
GROUP BY stock_code, description
HAVING SUM(quantity) FILTER (WHERE event_type = 'PURCHASE') > 10
ORDER BY return_volume_pct DESC
LIMIT 10;


-- 5. Customer Loyalty & Spend Tier Classification
SELECT
    customer_tier,
    COUNT(*)                                             AS customer_count,
    ROUND(AVG(total_spent), 2)                           AS avg_customer_spend,
    SUM(total_spent)                                     AS tier_revenue
FROM (
    SELECT
        customer_id,
        SUM(total_amount) AS total_spent,
        CASE
            WHEN SUM(total_amount) >= 1000 THEN 'VIP (>$1000)'
            WHEN SUM(total_amount) >= 500  THEN 'HIGH ($500-$1000)'
            WHEN SUM(total_amount) >= 100  THEN 'MEDIUM ($100-$500)'
            ELSE 'LOW (<$100)'
        END AS customer_tier
    FROM transactions
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
) tiers
GROUP BY customer_tier
ORDER BY tier_revenue DESC;


-- 6. Dead-Letter Queue (DLQ) Audit & Error Diagnostics
SELECT
    error_reason,
    COUNT(*)                                             AS total_failures,
    MIN(rejected_at)                                     AS first_failure,
    MAX(rejected_at)                                     AS latest_failure,
    (ARRAY_AGG(error_details))[1]                        AS sample_error_message
FROM dead_letter_queue
GROUP BY error_reason
ORDER BY total_failures DESC;
