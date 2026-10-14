select distinct
    event_hour,
    case
        when event_hour between 0 and 5 then 'night'
        when event_hour between 6 and 11 then 'morning'
        when event_hour between 12 and 17 then 'afternoon'
        else 'evening'
    end as day_part
from {{ ref('stg_transactions') }}