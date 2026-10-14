\set ON_ERROR_STOP on

INSERT INTO warehouse_raw.transactions_silver (
    transaction_id,
    step,
    transaction_type,
    amount,
    customer_id,
    recipient_id,
    event_time,
    event_date,
    is_fraud_actual,
    is_flagged_fraud,
    origin_balance_mismatch,
    destination_balance_mismatch,
    silver_batch_id,
    source_dataset_sha256
)
SELECT
    transaction_id,
    step,
    transaction_type,
    amount,
    customer_id,
    recipient_id,
    event_time_utc::timestamptz,
    event_date,
    is_fraud_actual,
    is_flagged_fraud,
    origin_balance_mismatch,
    destination_balance_mismatch,
    silver_batch_id,
    source_dataset_sha256
FROM warehouse_raw.transactions_silver_incoming
WHERE transaction_id IS NOT NULL
ON CONFLICT (transaction_id) DO NOTHING;

SELECT
    (SELECT count(*) FROM warehouse_raw.transactions_silver_incoming) AS incoming_rows,
    (SELECT count(*) FROM warehouse_raw.transactions_silver) AS warehouse_rows;