select
    transaction_id,
    step,
    transaction_type,
    amount,
    customer_id,
    recipient_id,
    event_time,
    event_date,
    event_hour,
    is_fraud_actual,
    is_flagged_fraud,
    origin_balance_mismatch,
    destination_balance_mismatch,
    silver_batch_id,
    source_dataset_sha256,
    warehouse_loaded_at,

    lag(event_time) over (
        partition by customer_id
        order by event_time, transaction_id
    ) as previous_transaction_at,

    count(*) over (
        partition by customer_id
        order by event_time, transaction_id
        rows between 4 preceding and current row
    ) as transactions_in_last_5_events,

    sum(amount) over (
        partition by customer_id
        order by event_time, transaction_id
        rows between 4 preceding and current row
    ) as amount_in_last_5_events,

    -- Batch analytics: RANGE gồm mọi transaction có cùng event_time mô phỏng.
    -- Không dùng trực tiếp làm feature online/ML vì peer cùng giờ có thể là tương lai.
    count(*) over (
        partition by customer_id
        order by event_time
        range between interval '24 hours' preceding and current row
    ) as transactions_in_24h_window

from {{ ref('stg_transactions') }}
