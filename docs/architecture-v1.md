# Kiến trúc hệ thống phiên bản 1

## Luồng dữ liệu

```text
PaySim CSV ──> sample/profile ──> replay producer ──> Redpanda transactions.raw
                                                        │
                                                        ▼
                                              PySpark Structured Streaming
                                                │                 │
                                             invalid        Bronze / Silver
                                                │                 │
                                                ▼                 ▼
                                           dead-letter      features + rules
                                                                  │
                                                                  ▼
                                                          ML scoring + decision
                                                                  │
                                             ┌────────────────────┴───────────────┐
                                             ▼                                    ▼
                                      decisions audit                    Gold / PostgreSQL
                                                                                   │
                                                                                   ▼
                                                                            Power BI Cloud

Delayed labels ──> fraud.labels ──> training snapshot ──> MLflow model registry
```

Tuần 1 chỉ triển khai phần profile, sample, contract và môi trường. Các khối còn lại là kiến trúc đích của roadmap; không được mô tả là đã chạy.

## Lựa chọn kỹ thuật

| Nhu cầu | Lựa chọn | Lý do |
|---|---|---|
| Giao dịch phát lại | Python producer | Kiểm soát seed, tốc độ, duplicate và late events |
| Event broker | Redpanda Kafka API | Single-node nhẹ hơn một Kafka stack nhiều dịch vụ trên laptop |
| Stream transform | Spark Structured Streaming local mode | Thể hiện event time, watermark, checkpoint và PySpark |
| Lakehouse | Iceberg trên filesystem local | Tách storage và table semantics; MinIO chỉ bật khi demo |
| Warehouse/BI marts | PostgreSQL + dbt | Star schema, SQL tests và kết nối Power BI qua curated snapshot |
| Online state | Redis | Velocity counters nhỏ; lịch sử đầy đủ lưu offline |
| ML | Scikit-learn baseline, model nâng cao, MLflow | Đánh giá theo thời gian, registry và versioning |
| Dashboard | Power BI Service/Fabric | Author trong browser trên macOS; upload Gold snapshot đã mask |

## Ranh giới dữ liệu

- Raw và Bronze bất biến để replay/audit.
- Silver chuẩn hóa và quarantine dữ liệu sai; không làm mất record.
- Gold chỉ gồm metrics, features hoặc quyết định đã kiểm chứng.
- Online features phải có timestamp; training joins chỉ dùng dữ liệu có sẵn trước event time.
- `isFraud` và `isFlaggedFraud` không vào transaction scoring event.

## Yêu cầu phi chức năng và giới hạn máy

- MacBook Pro M3, 8 GB RAM; Docker được cấp khoảng 4 GB. Chạy từng Compose profile, không bật toàn stack.
- Tuần 1: Python 3.12 ARM64 container, 2 CPU, memory limit 1 GB; DuckDB 2 threads, 768 MB và temp spill trên disk.
- Tuần 2 trở đi: Spark local[2] khi phát triển, hạn chế shuffle partitions và scale dữ liệu từ 100K lên 1M trước khi dùng full dataset.
- Throughput 100 event/giây và p95 dưới 5 giây là mục tiêu cần benchmark, chưa phải số đo.
- Pipeline phải chạy lại cùng batch mà không double-count; broker/consumer có retry và dead-letter path.
- Khi model không sẵn sàng, policy fallback về rule-only hoặc REVIEW, đồng thời ghi failure reason.
- Không mở PostgreSQL local ra Internet để Power BI Cloud truy cập; xuất Gold snapshot có version hoặc dùng Fabric Lakehouse.

## Quyết định còn mở

- Định dạng event trên Kafka: JSON cho MVP, sau đó cân nhắc Avro/Schema Registry nếu cần chứng minh compatibility.
- Chính sách late event/watermark sau khi đo phân bố độ trễ của replay simulator.
- Review capacity và ngưỡng score sau khi có validation set ở Tuần 7.
