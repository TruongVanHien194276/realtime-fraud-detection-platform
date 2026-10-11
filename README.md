# Real-time Fraud Detection Platform

Nền tảng phát hiện gian lận gần thời gian thực cho giao dịch mobile money. Dự án được thiết kế để thể hiện năng lực Data Engineering, Analytics Engineering, BI và ML Engineering trong cùng một sản phẩm có thể demo.

Tài liệu triển khai chính: [ROADMAP.md](ROADMAP.md). Hướng dẫn tải, kiểm tra và lấy mẫu PaySim nằm tại [docs/dataset-guide.md](docs/dataset-guide.md).

## Mục tiêu cuối

- Replay dữ liệu PaySim thành luồng giao dịch trên Redpanda/Kafka API.
- Xử lý event-time, duplicate và late event bằng PySpark Structured Streaming.
- Lưu Bronze, Silver và Gold dưới dạng Parquet/Apache Iceberg.
- Xây data warehouse PostgreSQL và mô hình dbt dạng star schema.
- Kết hợp fraud rules với mô hình ML để trả về `APPROVE`, `REVIEW` hoặc `BLOCK`.
- Theo dõi experiment và model version bằng MLflow; phục vụ điểm rủi ro qua FastAPI.
- Trình bày fraud operations và model monitoring bằng Power BI Service/Fabric trong trình duyệt; Streamlit chỉ dùng để kiểm tra KPI local trước khi đưa dữ liệu curated lên cloud.

## Cấu trúc thư mục

```text
airflow/       Batch orchestration và retraining DAGs
api/           FastAPI scoring service
dashboards/    Power BI Cloud specifications và Streamlit validation dashboard
data/          Local development data, không commit dữ liệu lớn
dbt/           Warehouse models, tests và documentation
docs/          Architecture, data contracts và decision records
infra/         Docker Compose và cấu hình dịch vụ
ml/            Training, evaluation và MLflow integration
monitoring/    Metrics và dashboard cấu hình
notebooks/     Chỉ dùng cho EDA và thử nghiệm
scripts/       Download, sampling và developer utilities
spark/         Batch và Structured Streaming jobs
src/           Python packages dùng chung
tests/         Unit, integration và data-quality tests
```

## Trạng thái

Hiện tại repository mới được khởi tạo cấu trúc và roadmap. Bắt đầu từ Tuần 1 trong `ROADMAP.md`.
