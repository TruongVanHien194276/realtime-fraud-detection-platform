-- Singular dbt test: trả về record lỗi; thành công nghĩa là 0 record.
select transaction_id
from {{ ref('fact_transactions') }}
where transactions_in_last_5_events not between 1 and 5
   or transactions_in_24h_window < 1
   or amount_in_last_5_events < amount
