\set ON_ERROR_STOP on

-- dbt --full-refresh replaces fact_transactions. Reapply these after every build.
CREATE INDEX IF NOT EXISTS idx_fact_fraud_recent
ON analytics_marts.fact_transactions (event_time DESC)
WHERE is_fraud_actual = 1;

CREATE INDEX IF NOT EXISTS idx_fact_customer_time
ON analytics_marts.fact_transactions (customer_id, event_time DESC);

ANALYZE analytics_marts.fact_transactions;
