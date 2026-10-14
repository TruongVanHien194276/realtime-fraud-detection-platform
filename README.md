# Real-time Fraud Detection Platform

Nền tảng phát hiện gian lận gần thời gian thực cho giao dịch mobile money. Dự án được thiết kế để thể hiện năng lực Data Engineering, Analytics Engineering, BI và ML Engineering trong cùng một sản phẩm có thể demo.

Tài liệu triển khai chính: [ROADMAP.md](ROADMAP.md). Hướng dẫn dữ liệu nằm tại [docs/dataset-guide.md](docs/dataset-guide.md); kết quả Tuần 1 gồm [yêu cầu nghiệp vụ](docs/business-requirements.md), [data contracts](docs/data-contracts.md), [kiến trúc v1](docs/architecture-v1.md) và [data profile](docs/data-profile.md).

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

Tuần 1 đã có raw manifest, samples, data contracts và profile. Tuần 2 có batch Bronze/Silver bằng Spark + Iceberg local; xem [thiết kế lakehouse](docs/lakehouse-design.md). Tuần 3 có PostgreSQL warehouse và dbt star schema trên mẫu 100k; xem [thiết kế warehouse và kết quả kiểm thử](docs/warehouse-model.md). Tuần 4 có Redpanda replay, Spark Structured Streaming, deduplication, watermark và DLQ; xem [ngữ nghĩa streaming và kết quả kiểm thử](docs/streaming-semantics.md). Raw CSV, Parquet samples, Iceberg warehouse, checkpoint và dbt artifacts không được commit vào Git.

## Chạy lại Tuần 1

Đặt PaySim CSV vào `data/raw/` theo [hướng dẫn dữ liệu](docs/dataset-guide.md), rồi chạy:

```bash
make build
make profile
make smoke-samples
make samples
make check
```

Nếu file CSV có tên khác, truyền `RAW_PATH=data/raw/ten-file.csv` cho các lệnh `make profile`, `make smoke-samples` và `make samples`. Môi trường phát triển chạy trên Docker ARM64 với 2 CPU và giới hạn 1 GB RAM. Manifest ghi rõ ngày tải là `not_recorded` vì thông tin này không được lưu tại thời điểm file được đặt vào project.

## Chạy lại Tuần 2

Chỉ bật Spark khi cần, không chạy đồng thời các workload nặng khác trên máy 8 GB. Build một lần:

```bash
make spark-build
```

Nạp sample 100k, tạo Silver rồi đối soát:

```bash
make bronze-100k
make silver
make verify-lakehouse
make check-spark
```

Bronze nhận thêm `--input-date YYYY-MM-DD` để backfill theo ngày; Silver nhận `--input-date` hoặc `--batch-id` để xử lý một phần. Cả hai có `--warehouse` để chạy trên warehouse riêng khi benchmark hoặc test; với Makefile dùng `SPARK_WAREHOUSE=data/tmp/ten-warehouse make bronze-100k`. `data/lakehouse/` là warehouse mặc định và được Git bỏ qua. `make check-spark` gồm cả integration test CSV lỗi → Iceberg quarantine → retry idempotent.

## Chạy lại Tuần 3

Sau khi đã có Silver 100k tại `data/tmp/week2-benchmark-100k-v1`, tạo `.env` với `POSTGRES_PASSWORD`, rồi chạy lần lượt:

```bash
make warehouse-up
docker compose --profile warehouse build dbt
make spark-build
make warehouse-export-100k
make warehouse-merge
make dbt-build
make dbt-docs
```

`make dbt-build` chạy 28 data tests và áp lại index sau build. Khi đổi window logic hoặc backfill ảnh hưởng lịch sử, dùng `make dbt-build DBT_FLAGS=--full-refresh`. Để xem dbt docs local, chạy `docker compose --profile warehouse run --rm -p 127.0.0.1:8081:8081 dbt dbt docs serve --host 0.0.0.0 --port 8081 --no-browser`, rồi mở `http://localhost:8081` (8080 có thể đang được Airflow dùng). Máy 8 GB nên chạy Spark và dbt theo từng job, không giữ chúng cùng lúc.

## Chạy lại Tuần 4

Dừng PostgreSQL/dbt trước khi chạy Spark streaming, sau đó khởi động broker và tạo topic:

```bash
docker compose --profile warehouse stop postgres
docker compose --profile streaming up -d redpanda
bash scripts/init_streaming_topics.sh
```

Chuẩn bị replay từ Silver 100k, rồi chạy `spark/stream_transactions.py` bằng `spark-submit` với Kafka connector Spark 3.5.7 và Iceberg JAR. Chạy producer ở terminal khác; sau khi xử lý xong 999 giao dịch, gửi ba probe `late`, `invalid`, `malformed` và kiểm tra bằng `tests/integration/check_week4_stream.py`. Lệnh đầy đủ, quy tắc checkpoint/restart, giới hạn at-least-once và số đo 100/500 EPS nằm ở [tài liệu streaming](docs/streaming-semantics.md).
