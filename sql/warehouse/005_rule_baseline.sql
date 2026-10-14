-- Run with psql -v run_id=... -v rule_version=... -v as_of=... -f this_file.
-- REVIEW/BLOCK count as alerts. Costs are illustrative assumptions, not PaySim facts.
\pset expanded on
WITH decisions AS (
    SELECT d.decision, d.amount, d.transaction_id, l.label
    FROM fraud_ops.decisions AS d
    LEFT JOIN fraud_ops.labels AS l
      ON l.run_id = d.run_id
     AND l.transaction_id = d.transaction_id
     AND l.label_observed_at <= :'as_of'::timestamptz
    WHERE d.run_id = :'run_id'
      AND d.rule_version = :'rule_version'
), counts AS (
    SELECT
        count(*) AS total_decisions,
        count(*) FILTER (WHERE label IS NOT NULL) AS labeled_decisions,
        count(*) FILTER (WHERE label = 1) AS fraud,
        count(*) FILTER (WHERE label = 0) AS legitimate,
        count(*) FILTER (WHERE label IS NOT NULL AND decision = 'REVIEW') AS reviews,
        count(*) FILTER (WHERE label IS NOT NULL AND decision = 'BLOCK') AS blocks,
        count(*) FILTER (WHERE label = 1 AND decision IN ('REVIEW', 'BLOCK')) AS tp,
        count(*) FILTER (WHERE label = 0 AND decision IN ('REVIEW', 'BLOCK')) AS fp,
        count(*) FILTER (WHERE label = 1 AND decision = 'APPROVE') AS fn,
        count(*) FILTER (WHERE label = 0 AND decision = 'APPROVE') AS tn,
        count(*) FILTER (WHERE label = 0 AND decision = 'BLOCK') AS false_blocks,
        coalesce(sum(amount) FILTER (WHERE label = 1 AND decision = 'APPROVE'), 0)
            AS missed_fraud_amount
    FROM decisions
)
SELECT
    :'run_id' AS run_id,
    :'rule_version' AS rule_version,
    :'as_of' AS label_as_of,
    total_decisions, labeled_decisions, fraud, legitimate,
    reviews, blocks, tp, fp, fn, tn,
    round(tp::numeric / nullif(tp + fp, 0), 4) AS alert_precision,
    round(tp::numeric / nullif(tp + fn, 0), 4) AS alert_recall,
    round(false_blocks::numeric / nullif(legitimate, 0), 4) AS false_block_rate,
    missed_fraud_amount,
    CASE WHEN labeled_decisions > 0
      THEN missed_fraud_amount + 5 * reviews + 10 * false_blocks
    END AS illustrative_cost
FROM counts;
