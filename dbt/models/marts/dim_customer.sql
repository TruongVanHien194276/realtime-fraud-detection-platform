select
    customer_id,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    count(*) as transaction_count
from {{ ref('stg_transactions') }}
group by customer_id