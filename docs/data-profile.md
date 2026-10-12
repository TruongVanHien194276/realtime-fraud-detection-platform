# Hồ sơ dữ liệu PaySim

Số liệu dưới đây được tạo bởi `scripts/profile_dataset.py` trên file PaySim trong `data/raw/`, với nguồn và checksum lưu tại `data/manifest.json`. Profile được tạo ngày 12/10/2026 UTC; thời điểm tải file gốc không được ghi lại riêng nên không suy đoán ngày tải từ thời gian tạo manifest.

## Quy mô và chất lượng

| Chỉ số | Kết quả |
|---|---:|
| Giao dịch | 6.362.620 |
| Fraud | 8.213 (0,129082%) |
| `isFlaggedFraud = 1` | 16 |
| `step` nhỏ nhất/lớn nhất | 1 / 743 |
| Amount trung bình | 179.861,90 |
| Amount trung vị xấp xỉ | 74.608,31 |
| Amount p95 xấp xỉ | 522.577,51 |
| Amount lớn nhất | 92.445.516,64 |
| Dòng trùng toàn bộ trường theo hash | 0 |
| Null ở 11 cột | 0 |
| Amount âm, step âm, nhãn/type không hợp lệ | 0 |

`approx_count_distinct` ước lượng khoảng 6,46 triệu giá trị `nameOrig` và 2,72 triệu `nameDest`. Đây là ước lượng có sai số, nên số đầu có thể nhỉnh hơn tổng số dòng; không dùng như exact count.

## Phân bố transaction type

| Type | Giao dịch | Fraud |
|---|---:|---:|
| CASH_OUT | 2.237.500 | 4.116 |
| PAYMENT | 2.151.495 | 0 |
| CASH_IN | 1.399.284 | 0 |
| TRANSFER | 532.909 | 4.097 |
| DEBIT | 41.432 | 0 |

Fraud trong PaySim chỉ xuất hiện ở CASH_OUT và TRANSFER. Đây là đặc điểm của dữ liệu mô phỏng; model phải được đánh giá theo thời gian và không coi kết quả là khả năng tổng quát trên hệ thống thật.

## Sample đã tạo

| File | Giao dịch | Fraud | Mục đích |
|---|---:|---:|---|
| `representative_10k.parquet` | 10.000 | 16 | Test và chạy nhanh |
| `representative_100k.parquet` | 100.000 | 137 | Developer loop |
| `representative_1m.parquet` | 1.000.000 | 1.336 | Integration benchmark |
| `fraud_scenario_10k.parquet` | 10.000 | 1.000 | Demo alert, không đánh giá model |

Sampling dùng seed 42; checksum từng Parquet được lưu trong `data/samples/sample_manifest.json`. Representative samples giữ tỷ lệ fraud gần dữ liệu gốc; scenario sample cố ý tăng lên 10% fraud nên không phản ánh phân bố thực.

## Việc cần theo dõi ở các tuần tiếp theo

- Kiểm tra tính nhất quán balance theo từng transaction type ở Silver; không loại bỏ record lạ khỏi raw.
- Ngăn label leakage từ `isFraud` và `isFlaggedFraud` khi replay.
- Dùng thời gian mô phỏng từ `step`, không gọi đây là timestamp giao dịch thật.
- Đánh giá model theo time split và PR-AUC/cost vì tỷ lệ fraud rất thấp.
