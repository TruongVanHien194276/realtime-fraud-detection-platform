# Hướng dẫn chạy từ đầu đến cuối

Chạy mọi lệnh ở **project root** trên Mac Apple Silicon. Đây là demo local trên máy 8 GiB RAM (Docker VM khoảng 4 GiB), không phải deployment production. Chạy từng pha; sau pha streaming hãy dừng Spark trước khi bật PostgreSQL/dbt, ML hoặc monitoring. Không dùng `docker compose down -v`: lệnh đó xóa dữ liệu PostgreSQL/Redpanda. Xem [runbook](docs/runbook.md) nếu một pha lỗi.

## 0. Chuẩn bị và kiểm tra môi trường

Cần Docker Desktop (Compose v2), `make`, Git và ít nhất vài chục GiB trống nếu xử lý PaySim 1m/full. Python 3.12 trên host chỉ cần cho Airflow; các bước còn lại dùng container ARM64. Tạo `.env` từ template và **đổi mật khẩu trước khi chạy PostgreSQL**. `.env` bị Git ignore.

```bash
cp .env.example .env
docker compose config --quiet
docker compose build dev spark
make check
make check-spark
```

`make check` phải pass Ruff và unit tests; `make check-spark` pass quality/integration tests. Docker image đầu tiên có thể tải dependency khá lâu. Nếu đã có `.env`, đừng ghi đè; kiểm tra `POSTGRES_PASSWORD` trong file của bạn. Các warehouse, cursor, checkpoint dưới đây là tên ví dụ cho **lần chạy đầu**; dùng tên mới cho một lần chạy độc lập, hoặc giữ đúng tên cũ khi cố ý resume. Không xóa checkpoint để “sửa” số đếm.

## 1. Smoke test không cần tải PaySim

Lộ trình này xác minh code từ một clean checkout, không phải benchmark/model training. Nếu đã chạy rồi, đổi đồng bộ `clean-demo` thành một tên mới.

```bash
docker compose run --rm dev python scripts/week10_demo_fixture.py \
  --output data/tmp/clean-demo.csv --rows 1000
DEMO_SHA="$(openssl dgst -sha256 data/tmp/clean-demo.csv | awk '{print $NF}')"
docker compose run --rm spark spark-submit --master 'local[2]' \
  --driver-memory 1g --jars /opt/iceberg/iceberg-spark-runtime.jar \
  spark/bronze_ingestion.py --input data/tmp/clean-demo.csv --format csv \
  --batch-id clean-demo --source-sha256 "$DEMO_SHA" \
  --warehouse data/tmp/clean-demo-lake
docker compose run --rm spark spark-submit --master 'local[2]' \
  --driver-memory 1g --jars /opt/iceberg/iceberg-spark-runtime.jar \
  spark/silver_transform.py --warehouse data/tmp/clean-demo-lake
docker compose run --rm spark spark-submit --master 'local[2]' \
  --driver-memory 1g --jars /opt/iceberg/iceberg-spark-runtime.jar \
  spark/verify_lakehouse.py --warehouse data/tmp/clean-demo-lake
```

Output cuối phải có `Bronze: 1000`, `Silver valid: 1000`, `Silver quarantine: 0`, `Duplicate IDs in Bronze: 0`, `RECONCILIATION PASSED`. Bản clean-clone đã kiểm chứng có [artifact](reports/week10/clean-checkout.json). Để chạy các pha tiếp theo cần PaySim thật (vẫn là dữ liệu **mô phỏng**).

## 2. PaySim → profile → sample → Bronze/Silver

Tải CSV theo [hướng dẫn nguồn và hợp đồng dữ liệu](docs/dataset-guide.md), đặt dưới `data/raw/`; không commit CSV hoặc Kaggle credentials. Thay tên file nếu khác mẫu:

```bash
export PAYSIM_RAW_PATH=data/raw/PS_20174392719_1491204439457_log.csv
test -f "$PAYSIM_RAW_PATH"
make profile RAW_PATH="$PAYSIM_RAW_PATH"
make samples RAW_PATH="$PAYSIM_RAW_PATH"
make bronze-100k SPARK_WAREHOUSE=data/tmp/week2-benchmark-100k-v1
make silver SPARK_WAREHOUSE=data/tmp/week2-benchmark-100k-v1
make verify-lakehouse SPARK_WAREHOUSE=data/tmp/week2-benchmark-100k-v1
```

Kỳ vọng `data/samples/representative_100k.parquet`, `representative_1m.parquet` và sample manifest; đối soát Bronze 100.000 = Silver valid + quarantine và zero duplicate ID. Raw CSV và Parquet samples bị ignore. Muốn kiểm tra 1m, dùng warehouse **khác**: `make bronze-1m SPARK_WAREHOUSE=data/tmp/lake-1m-local`, rồi `make silver` và `make verify-lakehouse` với cùng biến. Không dùng scenario/enriched sample để báo cáo hiệu năng model.

## 3. PostgreSQL warehouse → dbt Gold

Chỉ chạy sau khi Silver 100k đã có. PostgreSQL dùng named volume; `001_init.sql` tự chạy khi volume mới, còn `002_merge_incoming.sql` là bước merge lặp lại an toàn.

```bash
make warehouse-up
docker compose --profile warehouse build dbt
make warehouse-export-100k WAREHOUSE_SAMPLE=data/tmp/week2-benchmark-100k-v1
make warehouse-merge
make dbt-build
make dbt-docs
docker compose --profile warehouse exec -T postgres \
  psql -U fraud -d fraud_dw -c 'SELECT count(*) FROM analytics_marts.fact_transactions;'
```

Kỳ vọng Gold 100.000 dòng và dbt build 8 models + 28 tests pass trên sample chuẩn. Nếu logic cửa sổ hoặc backfill lịch sử thay đổi, dùng `make dbt-build DBT_FLAGS=--full-refresh` (target này áp lại index). [Warehouse model](docs/warehouse-model.md) ghi rõ grain, khóa và hạn chế point-in-time. Dừng DB trước Spark stream để tiết kiệm RAM: `docker compose --profile warehouse stop postgres` (không xóa volume).

## 4. Redpanda → Spark stream → rules/alerts

Build producer, khởi động broker và tạo topics; chuẩn bị fixture 1.000 dòng từ **Silver 100k**, không từ `clean-demo-lake`:

```bash
docker compose --profile streaming build producer
docker compose --profile streaming up -d redpanda
bash scripts/init_streaming_topics.sh
docker compose run --rm spark spark-submit \
  --jars /opt/iceberg/iceberg-spark-runtime.jar \
  spark/prepare_replay.py --warehouse data/tmp/week2-benchmark-100k-v1 \
  --output data/tmp/walkthrough/replay_1000 --limit 1000
```

**Terminal A — để Spark chạy liên tục** (chỉ một consumer dùng checkpoint này):

```bash
docker compose --profile streaming run --rm spark \
  spark-submit --driver-memory 1500m \
  --jars /opt/iceberg/iceberg-spark-runtime.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  --conf spark.jars.ivy=/workspace/data/tmp/ivy \
  spark/stream_transactions.py \
  --warehouse data/tmp/walkthrough/stream-lake \
  --checkpoint checkpoints/walkthrough/stream-v1 \
  --stream-id walkthrough-stream-v1 --max-offsets 200
```

**Terminal B — phát replay** sau khi Terminal A đã subscribe. Producer giữ lại row đầu để thử late event; vì vậy kỳ vọng **999 unique IDs** trong Silver, không phải 1.000.

```bash
docker compose --profile streaming run --rm producer \
  python src/producer/replay_transactions.py \
  --input data/tmp/walkthrough/replay_1000 \
  --cursor data/tmp/walkthrough/cursor-v1.json \
  --rate 100 --seed 42 --duplicate-rate 0.02
```

Chờ Spark xử lý hết backlog rồi gửi ba probe ở **Terminal B**, mỗi lệnh riêng với cùng `--input` như trên. Vẫn giữ Terminal A chạy:

```bash
docker compose --profile streaming run --rm producer \
  python src/producer/replay_transactions.py --input data/tmp/walkthrough/replay_1000 --mode late
docker compose --profile streaming run --rm producer \
  python src/producer/replay_transactions.py --input data/tmp/walkthrough/replay_1000 --mode invalid
docker compose --profile streaming run --rm producer \
  python src/producer/replay_transactions.py --input data/tmp/walkthrough/replay_1000 --mode malformed
```

Chờ thêm cho Spark ghi audit và DLQ, sau đó dừng Terminal A bằng Ctrl+C. Trong Terminal B, kiểm chứng:

```bash
docker compose --profile streaming run --rm spark \
  spark-submit --driver-memory 1500m \
  --jars /opt/iceberg/iceberg-spark-runtime.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  --conf spark.jars.ivy=/workspace/data/tmp/ivy \
  tests/integration/check_week4_stream.py \
  --warehouse data/tmp/walkthrough/stream-lake \
  --expected-valid 999 --require-dlq \
  --replay-dir data/tmp/walkthrough/replay_1000
```

`PASS` xác nhận 999 unique IDs và DLQ reasons. Topic/volume Redpanda có thể chứa lịch sử từ lần chạy trước; để benchmark độc lập dùng topic/checkpoint/warehouse phù hợp theo [streaming semantics](docs/streaming-semantics.md).

Khi Spark đã dừng, bật Postgres + Redis và chạy rules worker trên `transactions.validated`. `walkthrough-rules-v1` là ID của lần chạy này; nếu broker đã có lịch sử, diễn giải số lượng theo dữ liệu thực tế chứ không mặc định là 999:

```bash
docker compose --profile warehouse --profile rules --profile streaming up -d --wait postgres redis redpanda
docker compose --profile rules build rules
docker compose --profile warehouse exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U fraud -d fraud_dw < sql/warehouse/004_rules.sql
docker compose --profile rules run --rm rules \
  python -m fraud_platform.rules_worker \
  --run-id walkthrough-rules-v1 --config config/rules-v1.yaml --idle-seconds 10
docker compose --profile warehouse exec -T postgres \
  psql -U fraud -d fraud_dw -c \
  "SELECT decision, count(*) FROM fraud_ops.decisions WHERE run_id='walkthrough-rules-v1' GROUP BY decision ORDER BY decision;"
```

Các `fraud.alerts` là outbox/Kafka records; Kafka xuất at-least-once, còn decision ID trong DB chống duplicate theo run/policy. Để mô phỏng nhãn đến trễ và đo rule-only baseline, chạy hai lệnh `scripts/simulate_labels.py` (`--publish`, rồi `--drain-only`) và SQL `sql/warehouse/005_rule_baseline.sql` như [Week 5 evidence](docs/week5-rules.md), thay `run_id` nhất quán. Không trộn Gold 100k và decision population 999 làm cùng denominator.

## 5. BI snapshot, ML, API và monitoring

**BI local trước:** `make bi-build`, rồi xuất curated snapshot cho đúng rules run: `docker compose --profile bi run --rm bi python scripts/export_powerbi_snapshot.py --run-id walkthrough-rules-v1`. Lệnh in ra thư mục `data/exports/bi/<snapshot-id>/` và manifest. Tạo workbook bằng `docker compose --profile bi run --rm bi python scripts/build_powerbi_workbook.py data/exports/bi/<snapshot-id>` (thay ID vừa in). `make bi-preview` mở local Streamlit tại `http://127.0.0.1:8501`. Để publish Power BI Cloud, làm các thao tác browser/workspace trong [kế hoạch BI](docs/power-bi-cloud-plan.md), xác minh report/PDF thật; workbook local **không** phải dashboard cloud. Không upload raw CSV, key mask hoặc `.env`.

**ML sau khi tắt Spark/BI:** dùng sample 1m của Bước 2. Mỗi lệnh là một container hữu hạn:

```bash
docker compose --profile ml build ml
docker compose --profile ml run --rm ml python scripts/build_week7_features.py
docker compose --profile ml run --rm ml pytest tests/ml -q
docker compose --profile ml run --rm ml python scripts/train_week7.py
docker compose --profile ml run --rm ml python scripts/evaluate_week7_holdout.py
docker compose --profile ml run --rm ml python scripts/register_week8_model.py
```

Registration đặt alias `candidate`, **không tự promote thành `champion`**. Xem metric và artifact trong [Week 7 report](docs/week7-ml.md), chỉ quyết định promote sau khi review validation/holdout. API dùng alias `champion` lúc khởi động; nếu alias chưa có, API vẫn trả rule-only degraded. Để kiểm tra endpoint và fallback, `docker compose --profile api build api`, `docker compose --profile api up -d api`, rồi `curl -s http://127.0.0.1:8002/ready` và `curl -s http://127.0.0.1:8002/model-info`. Chạy `make check-api`; response `degraded=true` không phải model inference thành công.

**Airflow optional:** cần Python 3.12 host và virtualenv riêng (Airflow đang được project kiểm chứng ở 3.3.2). Chỉ tạo venv nếu `.venv/airflow` chưa tồn tại:

```bash
python3.12 -m venv .venv/airflow
.venv/airflow/bin/python -m pip install "apache-airflow==3.3.2" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
bash scripts/start_week9_airflow.sh
```

Official constraints URL đã được kiểm tra tồn tại; tải gói cần kết nối Internet. DAG `fraud_week9_batch_100k` chạy Bronze → Silver → quality gate → warehouse → dbt với retry; chỉ trigger khi sample 100k/Postgres đã sẵn sàng. Airflow UI `http://127.0.0.1:8082`; `airflow standalone` hiển thị thông tin đăng nhập lần đầu trong terminal, không commit thông tin đó. Không bật DAG song song với Spark stream. [Runbook](docs/runbook.md) giải thích failure/retry; DAG labels và candidate retraining là hai DAG riêng.

**Monitoring optional:** sau khi Spark/ML đã dừng, chạy `docker compose --profile monitoring --profile warehouse up -d fraud_exporter prometheus grafana`; Grafana `http://127.0.0.1:3009`, Prometheus `http://127.0.0.1:9099`. Xem panel `up` trước khi diễn giải lag. Dừng demo bằng `docker compose --profile monitoring stop grafana prometheus fraud_exporter`; không xóa volume. Renderer ảnh là profile riêng dùng tới 1 GiB RAM.

## 6. Xác minh đầu ra và giới hạn

[Báo cáo Week 10](reports/week10/performance.md) chứa benchmark 100k/1m, burst offered EPS 100/500/1.000, p50/p95/p99, API tuần tự và failure cases. Số EPS là tốc độ producer **được cấu hình**, không chứng minh sustained throughput; độ trễ stream chỉ đến **đầu micro-batch**, không đến quyết định fraud. [Kiến trúc](docs/architecture-final.md) chỉ ra các nhánh thực sự nối với nhau; hybrid API tách khỏi rules worker. [Portfolio guide](docs/portfolio-guide.md) gợi ý demo và CV bullets có bằng chứng.

GitHub Actions CI đã chạy thành công trên `main`; kiểm tra [lần chạy mới nhất](https://github.com/TruongVanHien194276/realtime-fraud-detection-platform/actions) sau mỗi lần push. Power BI Cloud report/PDF thật vẫn cần xác nhận trên đúng tài khoản/workspace; không được tự đánh dấu hoàn thành chỉ từ snapshot local.
