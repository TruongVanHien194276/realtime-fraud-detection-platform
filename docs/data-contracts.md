# Data contracts phiên bản 1

## Raw PaySim CSV

Nguồn raw được lưu nguyên trạng tại `data/raw/`; file hiện có SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`. Schema yêu cầu đúng 11 cột theo thứ tự dưới đây:

| Cột | Kiểu logic | Vai trò |
|---|---|---|
| `step` | integer không âm | Giờ mô phỏng của giao dịch |
| `type` | enum | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| `amount` | decimal không âm | Số tiền giao dịch |
| `nameOrig` | string | Tài khoản gửi |
| `oldbalanceOrg` | decimal | Số dư gửi trước giao dịch |
| `newbalanceOrig` | decimal | Số dư gửi sau giao dịch |
| `nameDest` | string | Tài khoản nhận |
| `oldbalanceDest` | decimal | Số dư nhận trước giao dịch |
| `newbalanceDest` | decimal | Số dư nhận sau giao dịch |
| `isFraud` | 0 hoặc 1 | Nhãn chỉ dùng sau quyết định |
| `isFlaggedFraud` | 0 hoặc 1 | Cờ từ dữ liệu gốc, không dùng làm input scoring |

Raw profile dùng DuckDB để khám phá kiểu cột; pipeline sản xuất sẽ dùng explicit schema. Nếu balance không khớp, giữ record và gắn quality flag ở Silver thay vì sửa raw.

## `transactions.raw` event

Contract code nằm trong `src/fraud_platform/contracts.py` (`TransactionEvent`). JSON dùng tên trường trong model. `schema_version = "1.0"`; không chấp nhận field lạ. Required fields: `event_id`, `transaction_id`, `event_time`, `ingested_at`, `step`, `transaction_type`, `amount`, `customer_id`, `recipient_id` và bốn giá trị balance.

Quy tắc mapping của producer Tuần 4:

1. `transaction_id` của PaySim batch là SHA-256 của `source_dataset_sha256` và `record_hash`. `record_hash` là SHA-256 của JSON canonical gồm 11 trường raw đã cast về schema thống nhất và `_corrupt_record`; không chứa đường dẫn file, `batch_id` hoặc thời điểm ingest. `spark/prepare_replay.py` đọc ID đã tính ở Silver Tuần 2 rồi mới bỏ label khỏi event, thay vì tính lại ID trong producer. Quy tắc content-hash này phù hợp với PaySim hiện tại (manifest báo không có duplicate toàn dòng); nguồn giao dịch thật cần ID bền vững do hệ thống nguồn cấp, vì hai giao dịch giống hệt nội dung vẫn phải phân biệt được.
2. `event_id` tạo ổn định từ cùng `transaction_id` để replay không biến một giao dịch cũ thành event mới.
3. `event_time = simulation_origin_utc + step giờ`, với `simulation_origin_utc` được cấu hình và ghi trong run manifest. Đây là thời gian mô phỏng, không phải thời gian giao dịch thật.
4. `ingested_at` là UTC lúc producer phát event.
5. `nameOrig` thành `customer_id`, `nameDest` thành `recipient_id`; không dùng nhãn `isFraud` hoặc `isFlaggedFraud` trong event.
6. `amount` và balances được serialize thành decimal string để tránh sai lệch do binary float.

Partition key của `transactions.raw` là `customer_id` để giữ thứ tự giao dịch của cùng khách hàng. Dùng `transaction_id` làm idempotency key ở consumer/sink. Event thiếu required field, sai type hoặc sai schema version phải vào dead-letter path với mã lỗi và raw payload có thể truy vết.

## `fraud.decisions` event

Contract code là `FraudDecisionEvent`. Mỗi decision có `event_id`, `transaction_id`, `decided_at`, `decision`, `risk_score` từ 0 đến 1, `triggered_rules`, `rule_version`, `feature_version`, `model_version` (nullable) và `processing_latency_ms` không âm. Tuần 5 thêm metadata tương thích: `run_id`, `source_event_time`, `feature_as_of_event_time`, `feature_computed_at` và `reason_codes`; worker điền đủ các trường này.

Một transaction chỉ có một quyết định hiệu lực cho mỗi policy version. Nếu cần rescoring, lưu một decision event mới có version và quan hệ supersedes, không âm thầm ghi đè audit trail.

## `fraud.labels` event

Nhãn đến trễ qua topic riêng và theo `FraudLabelEvent` gồm `run_id`, `transaction_id`, `label`, `label_observed_at`, `label_source`, `schema_version`. `label` là 0/1; `label_source` của Tuần 5 là `paysim_simulated_chargeback`. Consumer training chỉ lấy nhãn đã quan sát trước thời điểm snapshot.

## Quy tắc versioning

- Thêm field tùy chọn và giữ default là thay đổi tương thích.
- Đổi tên, đổi ý nghĩa hoặc loại bỏ field required là thay đổi breaking, cần schema version mới.
- Producer và consumer phải ghi schema version vào log, test và decision audit.
