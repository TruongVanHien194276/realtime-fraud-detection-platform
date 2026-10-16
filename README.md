# Real-time Fraud Detection Platform

Portfolio Data Engineering/ML Engineering cho giao dịch mobile money PaySim **mô phỏng**. Project có batch PySpark → Iceberg Bronze/Silver → PostgreSQL/dbt Gold; một nhánh replay Redpanda → Spark Structured Streaming → rules/Redis → quyết định và alert; MLflow + FastAPI phục vụ một nhánh hybrid scoring riêng.

**Bắt đầu tại [RUN.md](RUN.md)**: hướng dẫn lệnh chạy từ clean checkout đến BI/ML/API/monitoring, output kỳ vọng và lưu ý cho Mac Apple M3 8 GiB. Nếu chưa có PaySim CSV, Bước 1 trong RUN tạo 1.000 dòng synthetic để chạy thử Bronze/Silver ngay. Chạy `cp .env.example .env` và đặt mật khẩu local trước khi dùng PostgreSQL; không commit `.env` hoặc dữ liệu raw.

## Kiến trúc và ranh giới

[Sơ đồ triển khai và quyết định kỹ thuật](docs/architecture-final.md) phân biệt phần đã nối với phần mới là thiết kế. Kafka publication là at-least-once; Iceberg Silver merge idempotent theo `transaction_id`, không có bảo đảm exactly-once end-to-end. Rules worker **không gọi** hybrid API cho từng Kafka event. Power BI Cloud là một bước publish snapshot có mask lên tài khoản/workspace, không phải đường live từ PostgreSQL local.

## Bằng chứng có thể xem ngay

| Nhóm | Artifact |
|---|---|
| Benchmark và failure tests | [Báo cáo Week 10](reports/week10/performance.md), [JSON kết quả](reports/week10/), [clean-checkout smoke](reports/week10/clean-checkout.json) |
| Demo | [GIF minh chứng](demo/week10-evidence.gif), [ảnh Grafana](reports/screenshots/week9-grafana.png), [portfolio/CV guide](docs/portfolio-guide.md) |
| Chất lượng và ML | [Warehouse/dbt](docs/warehouse-model.md), [Week 7 temporal ML](docs/week7-ml.md), [Week 9 quality](reports/week9_data_quality.json) |
| Vận hành | [Runbook](docs/runbook.md), [streaming semantics](docs/streaming-semantics.md), [Power BI plan](docs/power-bi-cloud-plan.md) |

Benchmark batch lạnh 1 triệu dòng trên máy 8 GiB: 1 triệu Silver hợp lệ, zero duplicate/quarantine, tổng 385,69 giây. Ba đợt gửi ngắn ở cấu hình producer 100/500/1.000 EPS đều tạo 999 Silver IDs duy nhất; p95 đo từ `ingested_at` tới **đầu Spark micro-batch** là 41,67/71,71/49,00 giây. Đó không phải end-to-end decision latency hay sustained capacity. PaySim và mọi cost estimate trong dự án là giả lập.

## Trạng thái và giới hạn còn lại

- Batch, streaming, rule engine, ML baseline, registry, API, orchestration, data quality, Grafana và benchmark có code/bằng chứng local. GitHub Actions CI đã chạy thành công trên `main`; xem [trạng thái các lần chạy](https://github.com/TruongVanHien194276/realtime-fraud-detection-platform/actions).
- Workbook curated có mask và Streamlit local validation đã có. **Power BI Cloud report/PDF chưa có bằng chứng publish** trong repo; không mô tả Week 6 là đã hoàn tất phần cloud cho đến khi có report thật. Xem [hướng dẫn BI](docs/power-bi-cloud-plan.md).
- Một broker/partition/replica, một API worker và Docker VM khoảng 4 GiB không đại diện production HA. [Roadmap 10 tuần](ROADMAP.md) là kế hoạch lịch sử; kết quả thực tế nằm trong các artifact trên.

Repo không chứa raw PaySim CSV, checkpoint, warehouse local, `.env`, ML model hoặc token. Các file này được tạo lại theo [RUN.md](RUN.md). Gói phát hành cục bộ v1.0.0 và GIF là ảnh chụp bằng chứng tại thời điểm đo; sau mỗi lần thay đổi code, chạy lại test trước khi diễn giải số liệu cũ.
