# Batch lakehouse Bronze/Silver — Tuần 2

## Phạm vi và môi trường

- Nguồn: PaySim CSV bất biến trong `data/raw/` hoặc Parquet sample từ Tuần 1.
- Runtime: Spark 3.5.7, Python 3.11, Java 17, Iceberg 1.10.1 (Spark 3.5/Scala 2.12 runtime JAR), container ARM64.
- Laptop M3 8 GB, Docker khoảng 4 GB: chạy một Spark job mỗi lần, `local[2]`, driver 1 GB, container tối đa 2,7 GB, shuffle partitions 4. Không cần MinIO hay database cho Tuần 2.
- Catalog `lake` là Iceberg Hadoop catalog trên `data/lakehouse/`, không commit vào Git. Có thể dùng `--warehouse data/tmp/...` để test và benchmark tách biệt mà không xóa dữ liệu chính.

## Luồng dữ liệu

```text
PaySim CSV / representative Parquet
  -> explicit schema và kiểu logic thống nhất
  -> lake.bronze.transactions (Iceberg, partition event_date)
  -> quality checks, chuẩn hóa transaction_type
  -> lake.silver.transactions_valid
  -> lake.silver.transactions_quarantine (quality_errors)
```

CSV được kiểm tra đúng 11 tên cột và thứ tự trước khi Spark đọc bằng explicit schema `PERMISSIVE`. `amount` và balances là `Decimal(20,2)`; `step` là `Long`; labels là `Integer`. Parquet sample cũng được cast về cùng schema, để nội dung giao dịch có cùng hash khi đi qua hai định dạng. `event_time = 2025-01-01T00:00:00Z + step giờ`; đây là **thời gian mô phỏng**, không phải thời gian PaySim xảy ra ngoài đời. `event_date` là ngày UTC và là partition key; không partition theo account/customer có cardinality cao.

Bronze giữ 11 trường PaySim cùng `_corrupt_record`, `source_file`, `source_dataset_sha256`, `batch_id`, `loaded_at`, `record_hash`, `transaction_id`, `event_time`, `event_date`. `record_hash` là SHA-256 của JSON canonical từ 11 trường đã cast và `_corrupt_record`. `transaction_id` là SHA-256 của checksum nguồn gốc và record hash; không dùng tên file sample, batch ID hay loaded time. Manifest Tuần 1 xác nhận không có duplicate toàn dòng trong PaySim. Đây là key phục vụ dataset portfolio; nguồn giao dịch thật nên cung cấp ID riêng để phân biệt hai giao dịch giống hệt nội dung. Producer streaming Tuần 4 phải tính cùng ID từ raw row **trước khi bỏ hai label khỏi payload**.

Bronze và Silver dùng insert-only Iceberg `MERGE` theo `transaction_id`. Nguồn merge được deduplicate trước. Retry/backfill cùng dữ liệu không tăng số transaction, nhưng Iceberg vẫn có thể tạo snapshot metadata không thêm data file. `batch_id` và `loaded_at` của lần nạp đầu được giữ. Không dùng `overwritePartitions` khi backfill, vì có thể xóa giao dịch khác cùng ngày. Bronze nhận `--input-date` để chọn một event date; Silver nhận `--input-date` và/hoặc `--batch-id` để xử lý lại phần tương ứng. Nếu thay đổi quality policy, phải version/rebuild Silver có kiểm soát; insert-only merge không tự chuyển bản ghi giữa valid và quarantine.

Silver tách lỗi bắt buộc vào quarantine với mảng `quality_errors`: `CSV_PARSE_ERROR`, `INVALID_STEP`, `INVALID_AMOUNT`, `INVALID_TRANSACTION_TYPE`, `MISSING_ACCOUNT_ID`, `MISSING_BALANCE`, `INVALID_LABEL`, `INVALID_EVENT_TIME`. Balance không khớp công thức PaySim đơn giản chỉ gắn `origin_balance_mismatch`/`destination_balance_mismatch`; không xóa hay sửa giao dịch. `isFraud` và `isFlaggedFraud` được giữ để phân tích offline, **không dùng làm feature scoring trực tiếp**. Bronze đã unique theo ID, nên Silver không shuffle để deduplicate thêm lần nữa. Quality result được cache `DISK_ONLY` khi tách valid/quarantine để giữ RAM thấp; nếu quarantine rỗng, job tạo table rỗng mà không merge lại toàn bộ Bronze.

## Đối soát và giới hạn

- Sau khi chạy Silver cho toàn Bronze: `count(Bronze) = count(Silver valid) + count(Silver quarantine)`.
- Mỗi table có `transaction_id` unique; valid và quarantine không overlap.
- Sample 100k kỳ vọng 100.000 dòng và 137 fraud. Sample 1m kỳ vọng 1.000.000 dòng và 1.336 fraud. Quarantine sample sạch có thể bằng 0; fixture `tests/fixtures/paysim_quality.csv` chứng minh record sai thực sự được ghi vào quarantine với reason code.
- CLI backfill fixture đã được thử với `--input-date 2025-01-01` và `--batch-id week2-quality-fixture` trên warehouse tách biệt: Bronze 4, Silver valid 2, quarantine 2, không duplicate/overlap. Quarantine có `INVALID_AMOUNT` ở 2 record (amount âm và không parse được) và `CSV_PARSE_ERROR` ở 1 record.
- Khi chỉ xử lý một ngày/batch, đối soát toàn warehouse có thể chưa khớp cho đến khi các ngày/batch còn lại hoàn tất. `spark/verify_lakehouse.py` dành cho trạng thái đã hoàn tất.
- File `source_file` là đường dẫn vật lý của input đã đọc; không phải khóa định danh giao dịch.

## Chạy và kiểm tra

Các lệnh đầy đủ ở `README.md`. Chạy Bronze, Silver, `spark/verify_lakehouse.py`, rồi chạy lại cùng batch để chứng minh số dòng không tăng. `pytest tests/data_quality/test_silver_contract.py` kiểm tra invalid amount và balance flag. Benchmark nên dùng warehouse riêng cho từng kích thước để không nhầm fresh ingestion với incremental merge.

## Benchmark batch

Số liệu dưới đây đo ngày 2026-10-12 trên MacBook Pro M3 8 GB, Docker khoảng 4 GB, Spark container giới hạn 2,7 GB, `local[2]`, driver 1 GB. `elapsed_seconds` là thời gian bên trong job, không gồm thời gian Docker tạo container. Peak RAM lấy bằng `scripts/spark_with_peak.sh`, đọc cgroup peak trước khi `--rm` xóa container. Mỗi kích thước dùng warehouse mới để đo fresh ingestion; máy còn có container khác chạy nên đây là số đo thực nghiệm một lần, không phải cam kết hiệu năng.

| Dataset | Job | Input rows | Output rows | Seconds | Peak container RAM | Ghi chú |
|---|---|---:|---:|---:|---:|---|
| representative_100k | Bronze | 100.000 | 100.000 | 47,60 | 931,2 MiB | Fresh warehouse |
| representative_100k | Silver | 100.000 | 100.000 valid + 0 quarantine | 25,78 | 886,0 MiB | Cùng warehouse 100k |
| representative_1m | Bronze | 1.000.000 | 1.000.000 | 120,58 | 1.660,7 MiB | Fresh warehouse |
| representative_1m | Silver | 1.000.000 | 1.000.000 valid + 0 quarantine | 91,94 | 1.765,7 MiB | Cùng warehouse 1m |

Ngoài benchmark sample, trước khi tối ưu Silver, job full raw 6.362.620 dòng đã hoàn tất trong 1.267,95 giây và đối soát `6.362.620 = 6.362.620 + 0`. Số này **không** so sánh trực tiếp với benchmark Silver 1m vì job full raw là incremental merge vào Silver đã có 1m, chạy trên code trước thay đổi cache/dedup.
