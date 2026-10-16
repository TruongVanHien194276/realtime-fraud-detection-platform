# Trình bày portfolio và CV

## Demo phỏng vấn 5–7 phút

Trước khi trình bày, mở [sơ đồ kiến trúc](architecture-final.md), [báo cáo performance](../reports/week10/performance.md), ảnh Grafana và endpoint `/model-info`. Không để terminal lộ `.env`, token hay dữ liệu tài khoản raw. Trên máy 8 GiB, chạy Spark, ML training và Grafana renderer theo từng pha. Chỉ trình bày Power BI Cloud như report thật nếu đã publish và kiểm chứng trong workspace; Streamlit chỉ là local validation.

| Thời gian | Màn hình | Điểm cần nói |
|---|---|---|
| 0:00–0:45 | README và bài toán | PaySim là dữ liệu mô phỏng; đây là nền tảng có audit trail, không phải kết quả fraud production. |
| 0:45–1:35 | Kiến trúc | Bronze/Silver/Gold, streaming rules và hybrid API **riêng**; nêu rõ các nhánh chưa nối. |
| 1:35–2:30 | [Quality report](../reports/week9_data_quality.json), dbt graph | 100k dòng đối soát và 28 dbt tests; quality gate trước Gold. |
| 2:30–3:40 | Producer, stream audit, decision/alert | Normal replay, duplicate, late/malformed DLQ, historical fraud alert; Iceberg idempotent theo ID nhưng Kafka at-least-once. |
| 3:40–4:30 | [Grafana screenshot](../reports/screenshots/week9-grafana.png) | Health và decision; missing lag series không phải zero lag. BI Cloud chỉ trình bày nếu có artifact cloud thật. |
| 4:30–5:20 | `/model-info`, `/ready`, timeout test | Model version được pin lúc khởi động; hard rule ưu tiên; timeout trả về degraded rule-only. |
| 5:20–6:20 | Báo cáo performance | Batch 100k/1m, burst offered 100/500/1.000 EPS, p50/p95/p99 và API benchmark tuần tự; không gọi burst là sustained capacity. |
| 6:20–7:00 | ADR trong kiến trúc và runbook | Trade-off 8 GiB RAM, one broker/partition, hướng scale cloud. |

Câu hỏi nên luyện: ranh giới exactly-once (chỉ Silver merge idempotent), vì sao p95 ở 1.000 EPS thấp hơn 500 EPS (burst ngắn và lịch micro-batch khác, chưa chứng minh nguyên nhân), leakage ML (chỉ dùng lịch sử trước event), PostgreSQL outage (gate chặn Gold, Bronze giữ nguyên), timeout model (thread không bị kill), và trạng thái thật của Power BI Cloud.

## CV bullets có thể truy vết

Chỉ dùng các bullet có artifact trên commit phát hành; không thay số nếu chưa đo lại.

- Built a local fraud data platform with PySpark, Iceberg, Redpanda, PostgreSQL/dbt, Redis, MLflow and FastAPI; reconciled 100,000 Bronze/Silver/Gold rows and passed 28/28 dbt tests. [Evidence](../reports/week9_data_quality.json).
- Implemented replay-safe streaming ingestion with transaction-ID deduplication, event-time lateness, DLQ routing and checkpoint restart; verified 999 unique Silver IDs after duplicate/restart probes. [Evidence](../reports/week10/stream-failure.json).
- Built a version-pinned hybrid scoring API with hard-rule precedence and rule-only fallback when inference fails or times out. [API tests](../tests/unit/test_week10_api_timeout.py), [measurement scope](../reports/week10/performance.md).
- Benchmarked an isolated 1,000,000-row Bronze/Silver run on an 8 GiB Apple M3 laptop: 1,000,000 valid rows, zero quarantine/duplicate IDs and 385.69 seconds combined wall time (~2,593 rows/s). This is local cold-run throughput, not cluster throughput. [Evidence](../reports/week10/batch-1m-r1.json).
- Measured short 100/500/1,000 offered-EPS bursts, each producing 999 unique Silver IDs, with p95 producer-to-micro-batch-start delays of 41.67/71.71/49.00 seconds respectively. These **do not** prove sustained 1,000 EPS or subsecond decisions. [Evidence](../reports/week10/performance.md).

Không ghi “end-to-end exactly once”, “production HA”, “real-time under 5 seconds” hay “Power BI Cloud published” khi chưa có bằng chứng. PaySim amounts và fraud labels là mô phỏng; [Week 7 report](week7-ml.md) nêu giới hạn đánh giá model.
