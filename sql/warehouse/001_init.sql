CREATE SCHEMA IF NOT EXISTS warehouse_raw;

-- Bảng bền vững của warehouse: chỉ nhận giao dịch Silver hợp lệ.
CREATE TABLE IF NOT EXISTS warehouse_raw.transactions_silver (
    transaction_id text PRIMARY KEY,
    step bigint NOT NULL,
    transaction_type text NOT NULL,
    amount numeric(20, 2) NOT NULL,
    customer_id text NOT NULL,
    recipient_id text NOT NULL,
    event_time timestamptz NOT NULL,
    event_date date NOT NULL,
    is_fraud_actual smallint NOT NULL,
    is_flagged_fraud smallint NOT NULL,
    origin_balance_mismatch boolean NOT NULL,
    destination_balance_mismatch boolean NOT NULL,
    silver_batch_id text NOT NULL,
    source_dataset_sha256 text NOT NULL,
    warehouse_loaded_at timestamptz NOT NULL DEFAULT now()
);

ALTER DATABASE fraud_dw SET timezone TO 'UTC';