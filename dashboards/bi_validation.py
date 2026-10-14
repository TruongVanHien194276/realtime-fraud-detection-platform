"""Local validation/preview of the curated snapshot; not the Power BI report."""

import json
from pathlib import Path

import duckdb
import streamlit as st

ROOT = Path("data/exports/bi")
snapshots = sorted(ROOT.glob("*/manifest.json"), reverse=True)
st.set_page_config(page_title="Fraud operations snapshot", layout="wide")
st.title("Fraud operations snapshot")
st.caption("Historical PaySim replay. This local page validates the Power BI Cloud data model.")
if not snapshots:
    st.error("No snapshot. Run scripts/export_powerbi_snapshot.py in the bi service first.")
    st.stop()

chosen = st.selectbox("Snapshot", snapshots, format_func=lambda path: path.parent.name)
manifest = json.loads(chosen.read_text())
base = chosen.parent
db = duckdb.connect()


def table(name: str):
    return db.execute("SELECT * FROM read_parquet(?)", [str(base / f"{name}.parquet")]).df()


daily = table("fact_fraud_daily")
decisions = table("fact_fraud_decisions")
performance = table("fact_alert_performance")
timeline = table("fact_customer_timeline")
health = table("fact_pipeline_health")
st.caption(f"Run: {manifest['run_id']} · snapshot: {manifest['exported_at']}")

command, investigation, quality = st.tabs(
    ["Fraud Command Center", "Investigation Detail", "Platform & Data Quality"]
)
with command:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gold transactions", f"{int(daily.transaction_count.sum()):,}")
    c2.metric("Transaction value (simulated)", f"{daily.transaction_value.sum():,.0f}")
    c3.metric("Scored decisions", f"{len(decisions):,}")
    c4.metric(
        "Alert precision",
        f"{performance.tp.sum() / max(1, performance.tp.sum() + performance.fp.sum()):.2%}",
    )
    st.warning(
        "Gold population and scored run are different denominators. Do not compare as one cohort."
    )
    left, right = st.columns(2)
    with left:
        st.subheader("Decisions")
        st.bar_chart(decisions.groupby("decision").size())
    with right:
        st.subheader("Gold transaction value by day")
        st.line_chart(daily.groupby("event_date").transaction_value.sum())
    st.subheader("Daily scored alert outcomes")
    st.dataframe(performance, width="stretch", hide_index=True)

with investigation:
    alerts = decisions[decisions.alert_flag].sort_values("source_event_time", ascending=False)
    st.subheader("Alerts")
    st.dataframe(
        alerts[
            [
                "transaction_id",
                "customer_key",
                "source_event_time",
                "decision",
                "risk_score",
                "amount",
                "reason_codes",
                "label",
            ]
        ],
        width="stretch",
        hide_index=True,
    )
    customer = st.selectbox("Masked customer drill-through", sorted(alerts.customer_key.unique()))
    st.subheader("Customer timeline")
    st.dataframe(
        timeline[timeline.customer_key == customer].sort_values("event_time"),
        width="stretch",
        hide_index=True,
    )

with quality:
    c1, c2, c3 = st.columns(3)
    c1.metric("Snapshot age", "Historical replay")
    c2.metric("Ingest → decision p50", f"{int(health.latency_p50_ms.iloc[0]):,} ms")
    c3.metric("Ingest → decision p95", f"{int(health.latency_p95_ms.iloc[0]):,} ms")
    st.info(
        "Replay latency includes time waiting before the rules worker ran. "
        "It is not live serving latency."
    )
    st.subheader("Gold balance mismatch trend")
    st.line_chart(daily.groupby("event_date").balance_mismatch_count.sum())
    st.subheader("Snapshot lineage")
    st.json(
        {
            "snapshot_id": manifest["snapshot_id"],
            "run_id": manifest["run_id"],
            "tables": {name: item["rows"] for name, item in manifest["tables"].items()},
        }
    )
