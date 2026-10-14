{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

select
    transaction_id,
    step,
    customer_id,
    recipient_id,
    transaction_type,
    event_date,
    event_hour,
    event_time,
    amount,

    -- Ground-truth label chỉ dùng để phân tích/đánh giá offline.
    -- Tuyệt đối không đưa vào input của rule/model tại thời điểm scoring.
    is_fraud_actual,
    is_flagged_fraud,

    origin_balance_mismatch,
    destination_balance_mismatch,
    previous_transaction_at,
    transactions_in_last_5_events,
    amount_in_last_5_events,
    transactions_in_24h_window,
    silver_batch_id,
    source_dataset_sha256,
    warehouse_loaded_at

from {{ ref('int_transactions_enriched') }} as src

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} as existing
    where existing.transaction_id = src.transaction_id
)
{% endif %}
