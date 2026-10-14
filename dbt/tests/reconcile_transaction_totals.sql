-- Exact aggregate reconciliation catches a count match with mismatched amounts/labels.
with source_totals as (
    select
        count(*) as transaction_count,
        sum(amount) as total_amount,
        sum(is_fraud_actual) as fraud_count
    from {{ source('silver_export', 'transactions_silver') }}
),
fact_totals as (
    select
        count(*) as transaction_count,
        sum(amount) as total_amount,
        sum(is_fraud_actual) as fraud_count
    from {{ ref('fact_transactions') }}
)
select source_totals.*
from source_totals cross join fact_totals
where source_totals.transaction_count <> fact_totals.transaction_count
   or source_totals.total_amount <> fact_totals.total_amount
   or source_totals.fraud_count <> fact_totals.fraud_count
