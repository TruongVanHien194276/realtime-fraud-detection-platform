CREATE SCHEMA IF NOT EXISTS fraud_ops;

-- Chỉ giữ dữ liệu cần cho cửa sổ feature; không chứa nhãn fraud
-- run_id cho phép smoke và backtest cùng DB mà không lẫn state
CREATE TABLE IF NOT EXISTS fraud_ops.feature_events (
    run_id text NOT NULL,
    transaction_id text NOT NULL,
    customer_id text NOT NULL,
    recipient_id text NOT NULL,
    event_time timestamptz NOT NULL,
    amount numeric(20, 2) NOT NULL,
    PRIMARY KEY (run_id, transaction_id)
);

CREATE INDEX IF NOT EXISTS ix_feature_customer_time
ON fraud_ops.feature_events
(run_id, customer_id, event_time, transaction_id);

CREATE INDEX IF NOT EXISTS ix_feature_recipient_time
ON fraud_ops.feature_events
(run_id, recipient_id, event_time, transaction_id);

CREATE TABLE IF NOT EXISTS fraud_ops.decisions (
    run_id text NOT NULL,
    transaction_id text NOT NULL,
    rule_version text NOT NULL,
    decision_event_id uuid NOT NULL,
    source_event_key text NOT NULL,
    source_event_time timestamptz NOT NULL,
    feature_as_of_event_time timestamptz NOT NULL,
    feature_computed_at timestamptz NOT NULL,
    decided_at timestamptz NOT NULL,
    feature_version text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('APPROVE', 'REVIEW', 'BLOCK')),
    risk_score numeric(5, 4) NOT NULL,
    amount numeric(20, 2) NOT NULL,
    customer_id text NOT NULL,
    triggered_rules jsonb NOT NULL,
    reason_codes jsonb NOT NULL,
    features jsonb NOT NULL,
    processing_latency_ms bigint NOT NULL,
    PRIMARY KEY (run_id, transaction_id, rule_version),
    UNIQUE (decision_event_id)
);

-- Nâng cấp bảng đã tạo từ phiên bản đầu (cột `reason`) mà không mất dữ liệu.
-- Điều kiện giúp script chạy lại nhiều lần vẫn an toàn.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'fraud_ops'
          AND table_name = 'decisions'
          AND column_name = 'reason'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'fraud_ops'
          AND table_name = 'decisions'
          AND column_name = 'reason_codes'
    ) THEN
        ALTER TABLE fraud_ops.decisions RENAME COLUMN reason TO reason_codes;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_decisions_run_result
ON fraud_ops.decisions (run_id, rule_version, decision);

-- Transactional outbox: DB commit trước, Kafka publish sau
CREATE TABLE IF NOT EXISTS fraud_ops.outbox (
    id text PRIMARY KEY,
    run_id text NOT NULL,
    topic text NOT NULL,
    message_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_outbox_pending
ON fraud_ops.outbox (run_id, created_at)
WHERE published_at IS NULL;

-- Nhãn ở bảng riêng, chỉ dùng cho đánh giá sau quyết định
CREATE TABLE IF NOT EXISTS fraud_ops.labels (
    run_id text NOT NULL,
    transaction_id text NOT NULL,
    label smallint NOT NULL CHECK (label IN (0, 1)),
    label_observed_at timestamptz NOT NULL,
    label_source text NOT NULL,
    PRIMARY KEY (run_id, transaction_id)
);
