select
    transaction_id,
    step,
    transaction_type,
    amount,
    customer_id,
    recipient_id,
    event_time,
    event_date,
    extract(hour from event_time at time zone 'UTC')::int as event_hour,
    is_fraud_actual,
    is_flagged_fraud,
    origin_balance_mismatch,
    destination_balance_mismatch,
    silver_batch_id,
    source_dataset_sha256,
    warehouse_loaded_at
from {{ source('silver_export', 'transactions_silver') }}