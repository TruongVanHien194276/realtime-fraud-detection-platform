select
    recipient_id,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    count(*) as received_transaction_count
from {{ ref('stg_transactions') }}
group by recipient_id