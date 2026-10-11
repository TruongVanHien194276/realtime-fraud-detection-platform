# Hướng dẫn dữ liệu PaySim

PaySim là dữ liệu synthetic mô phỏng các giao dịch mobile money và hành vi gian lận. Dự án sử dụng PaySim để có đủ volume cho PySpark và streaming replay mà không xử lý dữ liệu tài chính cá nhân thật.

## Nguồn

- Bài báo gốc: [PaySim A Financial Mobile Money Simulator for Fraud Detection](https://www.msc-les.org/proceedings/emss/2016/EMSS2016_249.pdf)
- Bản dữ liệu thường được sử dụng trên Kaggle: [Synthetic Financial Datasets For Fraud Detection](https://www.kaggle.com/datasets/ealaxi/paysim1)

Việc tải file cần tuân thủ điều khoản của nguồn. Không đưa file CSV gốc vào GitHub.

## Vị trí file

Đặt file CSV sau khi tải vào:

```text
data/raw/
```

Tên file trong code không được hard-code. Khai báo đường dẫn qua configuration hoặc biến môi trường, ví dụ `PAYSIM_RAW_PATH`.

## Kiểm tra ngay sau khi tải

Ghi các thông tin sau vào `data/manifest.json`:

- Tên file.
- Nguồn tải.
- Ngày tải.
- Kích thước byte.
- SHA-256 checksum.
- Tổng số dòng, không tính header.
- Danh sách cột.
- Số lượng và tỷ lệ `isFraud = 1`.
- Số lượng và tỷ lệ `isFlaggedFraud = 1`.

Không ghi đường dẫn tuyệt đối hoặc thông tin tài khoản Kaggle vào manifest được commit.

## Sample strategy

Tạo sample bằng script, không copy/paste thủ công:

| Sample | Mục đích | Yêu cầu |
|---|---|---|
| 10.000 dòng | Unit test và schema test | Chạy trong vài giây |
| 100.000 dòng | Developer loop | Giữ đủ fraud cases để kiểm thử |
| 1 triệu dòng | Integration và performance | Không dùng hằng ngày |
| Full dataset | Batch benchmark cuối | Chỉ chạy sau khi pipeline ổn định |

Vì fraud rất hiếm, sample ngẫu nhiên đơn giản có thể chứa quá ít fraud. Script phải:

1. Giữ một sample phản ánh đúng phân bố gốc để đo metric thực tế.
2. Tạo thêm một scenario sample có nhiều fraud hơn chỉ để kiểm thử luồng alert.
3. Đặt tên rõ `representative` và `scenario`; không dùng scenario sample để báo cáo chất lượng model.
4. Dùng seed cố định và lưu seed trong manifest.

## Không biến label thành feature

Hai cột sau chỉ phục vụ label và benchmark:

- `isFraud`
- `isFlaggedFraud`

Không gửi chúng trong transaction event mà scoring service nhận. Delayed-label simulator sẽ phát label ở topic riêng sau khi decision đã được tạo, nhằm mô phỏng chargeback hoặc analyst confirmation.

## Kiểm tra chất lượng ban đầu

- Schema khớp danh sách cột mong đợi.
- `amount >= 0`.
- `step` không âm.
- `type` thuộc tập giá trị đã biết.
- `isFraud` và `isFlaggedFraud` chỉ nhận 0 hoặc 1.
- Ghi nhận nhưng không tự động xóa các trường hợp balance không khớp.
- Ghi duplicate rate dựa trên toàn bộ trường gốc trước khi tạo `transaction_id`.
- Không sửa file raw; mọi chuẩn hóa diễn ra từ Bronze sang Silver.

## Dữ liệu synthetic bổ sung

Các trường như `device_id`, `ip_country` hoặc `merchant_category` chỉ được thêm nếu:

- Có generator riêng với seed cố định.
- Document rõ đây là synthetic enrichment, không phải cột PaySim gốc.
- Không làm cho fraud label bị lộ trực tiếp.
- Generator không dựa vào `isFraud` trừ khi đang tạo một scenario test được đặt tên rõ.
