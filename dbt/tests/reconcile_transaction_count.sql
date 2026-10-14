with counts as (
    select
        (select count(*) from {{ source('silver_export', 'transactions_silver') }})
            as source_count,
        (select count(*) from {{ ref('fact_transactions') }})
            as fact_count
)
select *
from counts
where source_count <> fact_count