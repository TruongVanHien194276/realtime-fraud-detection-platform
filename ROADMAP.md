# Roadmap triển khai Real-time Fraud Detection Platform

Roadmap này xây dựng một hệ thống phát hiện gian lận gần thời gian thực từ dữ liệu PaySim, đi từ ingestion, lakehouse, data warehouse và Power BI đến rule engine, machine learning, model serving và monitoring. Kế hoạch ưu tiên khả năng chạy ổn định trên MacBook Pro M3 8 GB RAM, đồng thời tạo đủ bằng chứng kỹ thuật để trình bày trong CV và phỏng vấn.

## 1. Phạm vi và kết quả cuối cùng

Hệ thống nhận một giao dịch, kiểm tra data contract, bổ sung đặc trưng hành vi, chấm điểm rủi ro rồi trả về một trong ba quyết định:

```text
APPROVE  Giao dịch có rủi ro thấp
REVIEW   Giao dịch cần nhân viên fraud kiểm tra
BLOCK    Giao dịch vi phạm hard rule hoặc có rủi ro rất cao
```

Luồng tổng thể:

```text
PaySim CSV
   |
   v
Transaction replay producer
   |
   v
Redpanda transactions.raw
   |
   v
PySpark Structured Streaming
   |-- invalid event --> dead-letter topic
   |-- raw event -----> Bronze
   |-- clean event ---> Silver
   v
Feature calculation + rule engine + ML scoring
   |
   |-- decision events --> fraud.decisions
   |-- online state ----> Redis
   |-- analytics --------> PostgreSQL Gold marts
   v
Power BI Service / Fabric + MLflow + monitoring
```

Kết quả portfolio cuối cùng phải gồm:

- Repository có thể chạy lại từ đầu bằng tài liệu hướng dẫn rõ ràng.
- Data pipeline xử lý batch và streaming, có checkpoint và idempotency.
- Lakehouse Bronze, Silver và Gold.
- Data warehouse dạng star schema, dbt tests và dbt documentation.
- Fraud rules có reason code và audit trail.
- Mô hình ML được đánh giá theo thời gian, không dùng random split làm kết quả chính.
- MLflow experiment, model version và scoring API.
- Dashboard cho Fraud Operations, model monitoring và platform health.
- Benchmark throughput, latency, memory và chất lượng model.
- Architecture diagram, demo video và ba bullet dùng được trong CV.

## 2. Thiết kế theo cấu hình máy

### Cấu hình đã kiểm tra

| Thành phần | Giá trị |
|---|---:|
| Máy | MacBook Pro, Apple M3 ARM64 |
| CPU | 8 cores, gồm 4 performance và 4 efficiency |
| RAM vật lý | 8 GB |
| RAM Docker hiện tại | khoảng 4 GB |
| Dung lượng còn trống | khoảng 171 GB |
| Python trên host | 3.14.4 |
| Java trên host | OpenJDK 25 |
| Docker | đã cài đặt, Docker Engine 29.4.1 |

Python và Java trên host mới hơn phạm vi tương thích phổ biến của nhiều bản Spark/Airflow. Không cài trực tiếp các runtime dự án lên host; dùng container ARM64 và pin Python 3.11 hoặc 3.12 cùng Java 17 cho các workload cần Spark.

### Nguyên tắc tài nguyên

- Chỉ dùng Spark local mode: `local[2]` khi phát triển, tối đa `local[4]` khi benchmark.
- Giới hạn Spark driver khoảng 1.25–1.5 GB và shuffle partitions ở mức 4–8 cho dữ liệu mẫu.
- Không chạy Airflow, Spark, MLflow, Redpanda, MinIO và dashboard cùng lúc.
- Dùng Docker Compose profiles để bật từng nhóm dịch vụ.
- Dùng filesystem local cho Iceberg trong giai đoạn phát triển; MinIO chỉ là profile demo tùy chọn.
- PostgreSQL giới hạn khoảng 256–384 MB; Redis khoảng 128 MB; Redpanda khoảng 512–768 MB.
- Bắt đầu với 100.000 dòng, sau đó 1 triệu dòng. Chỉ chạy toàn bộ PaySim khi pipeline đã ổn định.
- Parquet phải partition theo ngày hoặc transaction step; không tạo quá nhiều file nhỏ.
- Sau mỗi buổi chạy nặng, tắt profile không dùng bằng Docker Compose để trả RAM cho macOS.

### Các profile dự kiến

| Profile | Dịch vụ chạy | Mục đích |
|---|---|---|
| `core` | PostgreSQL, Redpanda, Redis | Phát triển streaming và decision flow |
| `batch` | PostgreSQL, Spark job chạy một lần | Bronze, Silver, Gold và dbt |
| `orchestration` | PostgreSQL, Airflow standalone/local executor | Demo DAG, backfill và retraining |
| `ml` | PostgreSQL, MLflow, scoring API | Train, register và serve model |
| `observability` | Prometheus, Grafana, tùy chọn | Chỉ bật khi quay demo monitoring |
| `object-store` | MinIO, tùy chọn | Demo S3-compatible storage, không bắt buộc hằng ngày |

Không chạy quá hai profile cùng lúc trên cấu hình hiện tại.

## 3. Dataset và chiến lược dữ liệu

Nguồn và quy trình tải/lấy mẫu chi tiết được ghi tại `docs/dataset-guide.md`. File dữ liệu gốc phải được tải theo điều khoản của nguồn và không được commit vào Git.

### Dataset chính

Sử dụng PaySim, một bộ dữ liệu synthetic mô phỏng giao dịch mobile money. Các trường gốc quan trọng gồm:

- `step`: đơn vị thời gian mô phỏng.
- `type`: loại giao dịch như CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER.
- `amount`: số tiền giao dịch.
- `nameOrig`: tài khoản gửi.
- `oldbalanceOrg`, `newbalanceOrig`: số dư người gửi trước và sau.
- `nameDest`: tài khoản nhận.
- `oldbalanceDest`, `newbalanceDest`: số dư người nhận trước và sau.
- `isFraud`: nhãn fraud.
- `isFlaggedFraud`: cờ từ cơ chế hiện có trong dữ liệu.

### Các tầng dữ liệu

| Tầng | Nội dung | Định dạng |
|---|---|---|
| Raw | File PaySim gốc, bất biến | CSV, không commit Git |
| Sample | 100.000 dòng có phân bố fraud được ghi nhận | Parquet nhỏ |
| Bronze | Event đúng như nhận từ Kafka, thêm ingestion metadata | Iceberg/Parquet |
| Silver | Schema chuẩn, deduplicate, kiểm tra số dư và event time | Iceberg/Parquet |
| Gold | Feature aggregates, fraud decisions và BI marts | Iceberg + PostgreSQL |

### Chuyển dữ liệu batch thành stream

Producer đọc PaySim theo thứ tự `step`, tạo thêm:

- `event_id`: UUID duy nhất cho event.
- `transaction_id`: định danh giao dịch ổn định.
- `event_time`: thời gian mô phỏng quy đổi từ `step`.
- `ingested_at`: thời điểm producer phát event.
- `schema_version`: bắt đầu từ `1`.
- `device_id`, `ip_country`: trường synthetic có seed cố định để chạy lại cho cùng kết quả.

Producer phải hỗ trợ:

- Tốc độ tùy chỉnh: 10, 100, 500 và 1.000 event/giây.
- Replay từ offset hoặc transaction ID.
- Chèn duplicate có kiểm soát.
- Chèn late event có kiểm soát.
- Chèn event sai schema để kiểm tra dead-letter flow.
- Seed cố định để kết quả benchmark có thể lặp lại.

## 4. Công nghệ chốt cho dự án

| Nhu cầu | Công nghệ chính | Lý do |
|---|---|---|
| Streaming broker | Redpanda dùng Kafka API | Nhẹ hơn Kafka nhiều service, phù hợp laptop |
| Stream processing | PySpark Structured Streaming | Thể hiện PySpark, event time, watermark và checkpoint |
| Lakehouse | Apache Iceberg trên local filesystem; MinIO tùy chọn | Có ACID table format nhưng vẫn chạy được trên máy 8 GB |
| Warehouse | PostgreSQL | Kết nối tốt với dbt và Power BI |
| SQL modeling | dbt Core | Model, test, lineage và documentation |
| Orchestration | Airflow local executor/standalone | DAG, retry, backfill; chỉ bật riêng |
| Online feature state | Redis | Lưu counters và feature mới nhất với footprint nhỏ |
| ML | scikit-learn và LightGBM/XGBoost | Có baseline dễ giải thích và model mạnh |
| Experiment registry | MLflow | Theo dõi run, metric và model version |
| Model API | FastAPI | Endpoint scoring và health check |
| BI | Power BI Service/Fabric Cloud; Streamlit local để kiểm tra | Author report và semantic model trong browser, không phụ thuộc Power BI Desktop |
| Testing | pytest, dbt tests và custom data checks | Bao phủ code, SQL và dữ liệu |
| CI | GitHub Actions | Lint, unit test và dbt compile trên pull request |

Không thêm Feast, Kubernetes, Flink, Neo4j hoặc cloud trong phiên bản đầu. Feast và graph fraud là phần mở rộng sau khi hệ thống cốt lõi hoàn thành.

## 5. Kế hoạch 10 tuần

Kế hoạch mặc định là 8–12 giờ mỗi tuần. Nếu chỉ có 5–6 giờ mỗi tuần, giữ nguyên thứ tự và kéo mỗi tuần thành hai tuần lịch.

## Tuần 1: Chốt bài toán, lấy dữ liệu và dựng nền dự án

### Mục tiêu

Hiểu dữ liệu, định nghĩa fraud decision flow và tạo môi trường phát triển có thể lặp lại.

### Công việc

- Tải PaySim thủ công theo điều khoản nguồn dữ liệu và đặt file vào `data/raw/`.
- Ghi checksum, kích thước file, số dòng, tỷ lệ fraud và ngày tải vào data manifest.
- Viết script tạo ba sample: 10.000 dòng cho unit test, 100.000 dòng cho development và 1 triệu dòng cho integration benchmark.
- Profile null, duplicate, cardinality, phân bố transaction type, amount và fraud rate.
- Xác định business decisions: APPROVE, REVIEW, BLOCK.
- Định nghĩa cost giả định cho missed fraud, false block và manual review; tất cả phải được ghi là giả định.
- Viết data contract v1 cho transaction event và fraud decision event.
- Tạo kiến trúc cấp cao và danh sách non-functional requirements.
- Tạo Python project trong container với Python 3.11/3.12; thêm Ruff, pytest và pre-commit nếu cần.
- Viết `Makefile` hoặc task commands cho setup, sample và test.

### Công nghệ

Python, Pandas hoặc DuckDB cho profiling nhẹ, PyArrow, pytest, Docker.

### Output bắt buộc

- `docs/business-requirements.md`
- `docs/data-contracts.md`
- `docs/architecture-v1.md`
- `data/manifest.json`
- `scripts/create_samples.py`
- Báo cáo EDA ngắn trong `docs/data-profile.md`
- Unit tests cho sample generation và schema.

### Definition of done

- Có thể tạo lại cùng sample bằng cùng seed.
- Không commit file PaySim gốc.
- Biết chính xác tỷ lệ fraud và số dòng của từng sample.
- Mỗi event có ID, event time và schema version rõ ràng.

## Tuần 2: Batch lakehouse Bronze và Silver bằng PySpark

### Mục tiêu

Xây pipeline batch có thể chạy lại mà không nhân đôi dữ liệu.

### Công việc

- Tạo Spark container dùng Java 17 và Spark local mode.
- Đọc CSV raw bằng explicit schema; không dùng schema inference trong pipeline chính.
- Chuẩn hóa tên cột, data types và event time.
- Thêm ingestion metadata: source file, batch ID, loaded time và record hash.
- Ghi Bronze partition hợp lý, tránh partition theo customer có cardinality cao.
- Xây Silver transformations:
  - Loại duplicate theo transaction ID.
  - Kiểm tra amount không âm.
  - Kiểm tra balance consistency nhưng không xóa record lỗi khỏi audit trail.
  - Chuẩn hóa transaction type.
  - Tách valid và quarantine records.
- Viết job có tham số input date/batch ID để hỗ trợ backfill.
- Đo thời gian và peak memory cho 100.000 và 1 triệu dòng.

### Công nghệ

PySpark, Spark SQL, Parquet, Apache Iceberg local catalog.

### Output bắt buộc

- `spark/bronze_ingestion.py`
- `spark/silver_transform.py`
- `docs/lakehouse-design.md`
- `tests/data_quality/test_silver_contract.py`
- Bảng benchmark batch đầu tiên.

### Definition of done

- Chạy cùng batch hai lần không làm tăng số transaction hợp lệ.
- Record sai được lưu vào quarantine cùng reason code.
- Query đếm Bronze, Silver và quarantine khớp theo reconciliation rule.

## Tuần 3: Data warehouse, SQL và dbt

### Mục tiêu

Tạo semantic layer phục vụ BI và audit.

### Công việc

- Dựng PostgreSQL với memory limit thấp.
- Thiết kế star schema:
  - `fact_transactions`
  - `fact_fraud_decisions`
  - `fact_alerts`
  - `fact_chargebacks`
  - `fact_model_predictions`
  - `dim_customer`
  - `dim_recipient`
  - `dim_transaction_type`
  - `dim_rule`
  - `dim_model_version`
  - `dim_date` và `dim_time`
- Dùng dbt tạo staging, intermediate và mart models.
- Tạo incremental model cho fact tables.
- Thực hành window functions cho velocity và rolling statistics.
- Thêm dbt tests: unique, not null, relationships, accepted values và custom reconciliation.
- Tạo indexes cần thiết trong PostgreSQL và so sánh execution plan trước/sau.
- Sinh dbt documentation và lineage graph.

### Công nghệ

PostgreSQL, dbt Core, SQL, Spark-to-PostgreSQL export.

### Output bắt buộc

- dbt project chạy được.
- ERD/star-schema diagram.
- Ít nhất 12 data tests.
- `docs/warehouse-model.md`
- Báo cáo hai query tuning cases có `EXPLAIN ANALYZE`.

### Definition of done

- `dbt build` thành công trên sample 100.000 dòng.
- Fact và dimension keys không bị orphan ngoài các trường hợp đã document.
- Dashboard queries có thời gian phản hồi phù hợp trên sample local.

## Tuần 4: Streaming ingestion và tính đúng đắn của event

### Mục tiêu

Replay PaySim thành stream và xử lý đúng duplicate, late event và lỗi schema.

### Công việc

- Dựng Redpanda single-node và topic configuration.
- Tạo các topic:
  - `transactions.raw`
  - `transactions.validated`
  - `transactions.invalid`
  - `fraud.decisions`
  - `fraud.alerts`
  - `fraud.labels`
- Viết transaction replay producer có cấu hình tốc độ và seed.
- Đọc Kafka API bằng Spark Structured Streaming.
- Parse JSON/Avro theo explicit schema và schema version.
- Dùng `event_time`, watermark và transaction ID để deduplicate.
- Lưu checkpoint ngoài thư mục output.
- Ghi invalid event vào dead-letter topic với error code.
- Thử restart Spark job để chứng minh không double-count.
- Đo p50/p95 processing delay ở 100 và 500 event/giây.

### Công nghệ

Redpanda, Kafka API, PySpark Structured Streaming, checkpointing.

### Output bắt buộc

- `src/producer/replay_transactions.py`
- `spark/stream_transactions.py`
- Topic initialization script.
- `docs/streaming-semantics.md`
- Integration test cho duplicate, late event và malformed event.

### Definition of done

- Producer có thể pause/resume/replay có kiểm soát.
- Spark restart không tạo quyết định trùng cho cùng transaction.
- Invalid events xuất hiện trong dead-letter topic và vẫn truy vết được source event.

## Tuần 5: Real-time features và rules engine

### Mục tiêu

Tạo hệ thống fraud detection đầu tiên chưa cần machine learning nhưng có thể giải thích và audit.

### Công việc

- Xây các feature theo customer và recipient:
  - Transaction count và amount sum trong 5 phút, 1 giờ và 24 giờ.
  - Thời gian từ giao dịch trước.
  - Số recipient khác nhau gần đây.
  - Amount so với average/median lịch sử.
  - Amount-to-balance ratio.
  - Recipient fan-in và fan-out.
- Lưu state nhỏ trong Redis; lịch sử đầy đủ vẫn nằm ở Silver/Gold.
- Định nghĩa hard rules và soft rules bằng configuration, không hard-code rải rác.
- Mỗi rule phải có rule ID, version, threshold và reason code.
- Xây decision policy APPROVE/REVIEW/BLOCK.
- Ghi decision event gồm feature timestamp, triggered rules và latency.
- Tạo delayed-label simulator để phát chargeback/fraud labels sau một khoảng thời gian.

### Công nghệ

PySpark, Redis, Python rules engine, PostgreSQL.

### Output bắt buộc

- Feature specification.
- Versioned rule configuration.
- Rules engine với unit tests.
- Decision events và audit table.
- Báo cáo precision/recall/cost của rule-only baseline.

### Definition of done

- Có thể giải thích vì sao từng transaction bị REVIEW hoặc BLOCK.
- Feature tại thời điểm quyết định không sử dụng dữ liệu tương lai.
- Rules có thể thay threshold mà không sửa source code.

## Tuần 6: Dashboard vận hành trên Power BI Cloud

### Mục tiêu

Biến dữ liệu thành công cụ cho fraud analyst và stakeholder bằng Power BI Service/Fabric trong trình duyệt, không phụ thuộc Power BI Desktop hoặc Windows VM.

### Công việc

- Xây Streamlit validation page tối giản để kiểm tra query và KPI trên Mac; đây không phải dashboard portfolio chính.
- Kiểm tra tài khoản tại `app.powerbi.com`, license và quyền tạo workspace/semantic model.
- Chọn một trong hai luồng Power BI Cloud, ưu tiên theo thứ tự:
  - Luồng chính nếu có Fabric trial/capacity: export Gold marts thành Parquet, đưa vào Fabric Lakehouse/OneLake, tạo semantic model và report bằng web authoring. Dùng Direct Lake nếu workspace hỗ trợ.
  - Luồng fallback vẫn hoàn toàn cloud: export các Gold marts nhỏ thành một workbook Excel có các bảng rõ ràng, upload vào Power BI Service, tạo semantic model và report trong browser.
- Không kết nối trực tiếp Power BI Cloud đến PostgreSQL đang chạy trên Mac vì sẽ cần giải quyết network exposure/gateway và làm tăng độ phức tạp không cần thiết cho v1.
- Xây ba trang report:
  - Fraud Command Center.
  - Investigation Detail.
  - Platform and Data Quality.
- Định nghĩa KPI:
  - Transaction value.
  - Suspected fraud amount.
  - Approval/review/block rate.
  - Alert precision.
  - Fraud amount captured.
  - False-positive rate.
  - Data freshness và processing latency.
- Tạo customer timeline và drill-through từ alert đến transaction.
- Viết DAX measure specification và relationship map trước khi xây semantic model.
- Chỉ upload curated Gold marts; không upload raw/Bronze/Silver hoặc customer ID chưa mask.
- Tạo relationships, measures, date table và report hoàn toàn trong web editor.
- Tạo script `scripts/export_powerbi_snapshot.py` để xuất một snapshot có version và manifest.
- Nếu dùng Fabric Lakehouse, lưu notebook/pipeline hoặc hướng dẫn upload để quá trình có thể lặp lại.
- Lưu screenshot và export PDF để recruiter vẫn xem được khi link Power BI không công khai hoặc trial hết hạn.
- Ghi rõ license/capacity đang dùng; không phụ thuộc Copilot vì trial/free capacity có thể không hỗ trợ.

### Công nghệ

Power BI Service, Microsoft Fabric/OneLake nếu khả dụng, DAX, Excel hoặc Parquet snapshot, Streamlit validation.

### Output bắt buộc

- Streamlit validation page chạy local.
- Power BI semantic model và report được tạo trong Power BI Service/Fabric.
- Versioned Power BI data snapshot và manifest.
- `docs/power-bi-cloud-plan.md`
- `docs/kpi-catalog.md`
- Dashboard screenshots và PDF export.

### Definition of done

- Mọi KPI có SQL/dbt source và business definition.
- Có thể drill từ tổng quan đến transaction cụ thể.
- Dashboard không tiết lộ customer ID thô trong ảnh portfolio; dùng masked ID.
- Report vẫn có thể phục dựng từ Gold snapshot nếu Fabric trial hoặc workspace bị mất.

## Tuần 7: Machine learning baseline và temporal validation

### Mục tiêu

Xây model có đánh giá đáng tin cậy và so sánh công bằng với rules.

### Công việc

- Tạo training dataset bằng point-in-time feature joins.
- Split theo thời gian thành train, validation và test; không random split làm kết quả chính.
- Kiểm tra label leakage và feature availability.
- Train các baseline:
  - Rule-only.
  - Logistic Regression với class weighting.
  - Random Forest hoặc HistGradientBoosting.
  - LightGBM/XGBoost nếu ARM64 environment ổn định.
- Không dùng accuracy làm metric chính.
- Đánh giá:
  - PR-AUC.
  - Recall tại fixed false-positive rate.
  - Precision tại top K alerts.
  - Fraud amount captured.
  - Expected business cost.
- Chọn threshold dựa trên capacity giả định của fraud-review team.
- Tạo feature importance hoặc SHAP trên sample giới hạn để tránh tốn RAM.

### Công nghệ

Pandas/Polars cho sample, scikit-learn, LightGBM/XGBoost tùy compatibility, MLflow tracking.

### Output bắt buộc

- Reproducible training script, không chỉ notebook.
- Feature list và leakage checklist.
- Model comparison report.
- Confusion matrix theo threshold đã chọn.
- MLflow experiment với params, metrics và artifacts.

### Definition of done

- Model đánh bại rule-only baseline trên metric kinh doanh đã chọn hoặc có phân tích trung thực vì sao không.
- Test set không được dùng để chọn feature hoặc threshold.
- Training chạy được trong giới hạn RAM bằng sample/column selection.

## Tuần 8: Model registry, API và hybrid decisioning

### Mục tiêu

Đưa model vào luồng quyết định có versioning và fallback an toàn.

### Công việc

- Đăng ký model vào MLflow Model Registry.
- Sử dụng aliases như `candidate` và `champion` thay cho đường dẫn model cố định.
- Xây FastAPI endpoints:
  - `GET /health`
  - `GET /ready`
  - `POST /score`
  - `GET /model-info`
- Định nghĩa request/response schema và validation.
- Kết hợp hard rules với model score:
  - Hard rule nghiêm trọng có thể BLOCK.
  - Model score điều phối APPROVE/REVIEW/BLOCK.
  - Khi API/model lỗi, fallback về rule-only hoặc REVIEW theo policy.
- Ghi model version, feature version, threshold và reason codes vào decision event.
- Test training-serving skew bằng golden transactions.
- Load test API ở mức phù hợp laptop, không chạy song song toàn stack.

### Công nghệ

MLflow, FastAPI, Pydantic, Redis, pytest.

### Output bắt buộc

- Versioned registered model.
- Dockerized scoring API.
- OpenAPI documentation.
- Golden test dataset.
- Báo cáo API latency p50/p95 và error rate.

### Definition of done

- Cùng input và cùng model version trả cùng kết quả.
- API vẫn có quyết định fallback khi model registry tạm không truy cập được.
- Mỗi prediction có thể truy về model, feature và rule versions.

## Tuần 9: Orchestration, data quality và monitoring

### Mục tiêu

Vận hành pipeline như một hệ thống production thu nhỏ.

### Công việc

- Tạo Airflow DAGs cho:
  - Batch ingest/backfill.
  - Silver/Gold transformation.
  - dbt build và tests.
  - Daily label aggregation.
  - Scheduled retraining.
  - Model evaluation report.
- Cấu hình retry, timeout, SLA/freshness check và idempotent task boundaries.
- Không dùng Airflow để giữ một Spark streaming job chạy vĩnh viễn; quản lý streaming process riêng.
- Thêm data-quality checks cho schema, uniqueness, referential integrity, volume và freshness.
- Xuất metrics:
  - Kafka consumer lag.
  - Events processed/rejected.
  - End-to-end latency.
  - Decision rate.
  - API latency/error.
  - Feature null rate.
- Bật Prometheus/Grafana chỉ trong phiên demo monitoring.
- Viết runbook cho broker down, corrupted checkpoint, database unavailable và schema mismatch.

### Công nghệ

Airflow, dbt tests, pytest/custom checks, Prometheus, Grafana.

### Output bắt buộc

- Airflow DAGs có screenshot run thành công.
- Data-quality report.
- Monitoring dashboard screenshot.
- `docs/runbook.md`
- Một incident postmortem giả lập.

### Definition of done

- Có thể backfill một batch mà không duplicate.
- Một quality check thất bại sẽ ngăn publish Gold nhưng không xóa Bronze.
- Runbook nêu rõ cách phát hiện, giảm tác động và phục hồi.

## Tuần 10: Benchmark, hardening và đóng gói portfolio

### Mục tiêu

Chuyển project kỹ thuật thành bằng chứng tuyển dụng dễ xem và dễ trình bày.

### Công việc

- Chạy benchmark theo ba mức:
  - 100.000 dòng cho developer loop.
  - 1 triệu dòng cho integration.
  - Toàn bộ dataset cho batch benchmark nếu máy ổn định.
- Đo throughput ở 100, 500 và 1.000 event/giây; giảm mức cuối nếu Docker memory pressure cao và ghi rõ giới hạn.
- Ghi CPU, RAM, latency p50/p95/p99, Kafka lag và failure count.
- Chạy failure scenarios:
  - Duplicate event.
  - Late event.
  - Malformed event.
  - Spark restart.
  - PostgreSQL tạm dừng.
  - Model API timeout.
- Hoàn thiện README với architecture diagram và quickstart.
- Tạo demo script 5–7 phút.
- Quay video hoặc GIF gồm normal transaction, fraud burst, alert, dashboard và model version.
- Viết architecture decision records về Spark, Redpanda, Iceberg và hybrid rules-plus-model.
- Viết CV bullets chỉ dùng số benchmark đã đo thật.
- Tạo release tag `v1.0.0` sau khi test từ clean checkout.

### Công nghệ

Docker Compose, pytest, benchmark scripts, GitHub Actions, diagram tool.

### Output bắt buộc

- Performance report.
- Architecture diagram cuối.
- Demo video/GIF và screenshots.
- README hoàn chỉnh.
- `docs/interview-guide.md`
- `docs/cv-bullets.md`
- GitHub release hoặc local release bundle.

### Definition of done

- Người khác có thể chạy happy path theo README mà không hỏi tác giả.
- Mọi con số trong CV truy được về benchmark artifact.
- Có thể giải thích trade-off, failure modes, giới hạn laptop và hướng scale lên cloud.

## 6. Backlog ưu tiên

### Must have

- PaySim replay producer.
- Redpanda/Kafka topics.
- PySpark batch và Structured Streaming.
- Bronze/Silver/Gold.
- PostgreSQL star schema và dbt tests.
- Rule engine và audit trail.
- Temporal ML evaluation.
- MLflow và FastAPI.
- Power BI Cloud deliverable và Streamlit local validation.
- Integration tests, benchmark và documentation.

### Should have

- Redis online state.
- Delayed label simulator.
- Airflow retraining DAG.
- Prometheus/Grafana demo.
- Optional MinIO profile.
- SHAP explanations trên sample.

### Could have sau v1

- Feast feature store.
- Graph features hoặc Neo4j.
- Unsupervised anomaly model.
- Champion/challenger shadow scoring.
- Drift-triggered retraining.
- Terraform và deployment lên cloud.
- LLM assistant cho fraud investigator có guardrails.

## 7. Metrics và mục tiêu ban đầu

Các mục tiêu dưới đây là target để kiểm thử, không phải kết quả được tuyên bố trước:

| Nhóm | Metric | Target phát triển |
|---|---|---:|
| Streaming | Sustained input rate | 100 event/giây ổn định |
| Streaming | Stretch input rate | 500–1.000 event/giây nếu RAM cho phép |
| Latency | p95 end-to-end local | dưới 5 giây ở mức 100 event/giây |
| Correctness | Duplicate decisions | 0 trong test scenario |
| Quality | Invalid event routing | 100% vào dead-letter path |
| Reliability | Restart recovery | không double-count sau restart |
| ML | Primary metric | PR-AUC và business cost, không dùng accuracy |
| Operations | Review capacity | được cấu hình và dùng khi chọn threshold |
| Reproducibility | Sample/model rerun | cùng seed cho cùng kết quả trong tolerance |

Target phải được điều chỉnh sau benchmark và không được đưa vào CV như kết quả thực tế.

## 8. Quy trình làm việc mỗi tuần

Mỗi tuần đi theo cùng một vòng lặp:

1. Chốt acceptance criteria trước khi code.
2. Tạo sample nhỏ và unit test trước.
3. Chạy local với profile tối thiểu.
4. Thêm integration test cho happy path và ít nhất một failure path.
5. Đo runtime/memory thay vì phỏng đoán.
6. Cập nhật architecture/data contract nếu implementation thay đổi.
7. Chụp output quan trọng phục vụ portfolio.
8. Ghi decision và trade-off trong `docs/`.
9. Chỉ sau đó mới tăng data volume.

Quy tắc hoàn thành một pull request hoặc milestone:

- Code format/lint đạt.
- Unit tests đạt.
- Data reconciliation đạt.
- Không commit secrets hoặc raw dataset.
- README hoặc tài liệu liên quan được cập nhật.
- Có bằng chứng output cho phần vừa hoàn thành.

## 9. Câu chuyện trình bày trong phỏng vấn

Luồng kể dự án nên là:

1. Fraud labels đến trễ và dữ liệu mất cân bằng khiến bài toán không chỉ là classification.
2. Hệ thống sử dụng event-time, watermark, checkpoint và idempotency để bảo đảm tính đúng đắn.
3. Lakehouse giữ raw audit history; warehouse phục vụ BI với star schema.
4. Rules cung cấp guardrail và explainability; ML cải thiện khả năng xếp hạng rủi ro.
5. Temporal split và point-in-time features ngăn leakage.
6. Threshold được chọn theo chi phí và review capacity, không theo accuracy.
7. Toàn bộ decision lưu model/rule/feature version để audit.
8. Kiến trúc local được chia profile do giới hạn RAM; khi scale có thể tách broker, Spark cluster, object storage, feature store và serving layer.

## 10. Bước bắt đầu ngay

Buổi làm việc đầu tiên chỉ cần hoàn thành năm việc:

1. Tải PaySim và đặt vào `data/raw/`.
2. Ghi checksum và số dòng.
3. Tạo sample 10.000 và 100.000 dòng với seed cố định.
4. Viết data contract cho `transactions.raw`.
5. Chạy test schema đầu tiên.

Không dựng toàn bộ Docker stack trong ngày đầu. Pipeline dữ liệu và contract phải đúng trước khi thêm broker, Spark streaming hoặc machine learning.
