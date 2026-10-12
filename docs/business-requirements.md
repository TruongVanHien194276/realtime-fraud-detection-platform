# Yêu cầu nghiệp vụ cho phát hiện gian lận

## Bối cảnh và phạm vi

Một nền tảng mobile money cần đánh giá mỗi giao dịch khi sự kiện đến, trả quyết định `APPROVE`, `REVIEW` hoặc `BLOCK`, và lưu lý do để nhân viên fraud điều tra. Dữ liệu PaySim là dữ liệu mô phỏng; project này chứng minh kiến trúc và quy trình, không đại diện cho hiệu quả trên giao dịch ngân hàng thật.

Trong phiên bản đầu, hệ thống nhận giao dịch PaySim được phát lại theo thời gian mô phỏng, kiểm tra schema, tính behavioral features, áp dụng rules và sau đó tích hợp risk score của model. Nhãn `isFraud` chỉ đi vào nhánh đánh giá/training sau khi quyết định đã được tạo.

## Người dùng và câu hỏi cần trả lời

| Vai trò | Nhu cầu |
|---|---|
| Fraud analyst | Xem alert, lý do, lịch sử giao dịch và quyết định cuối |
| Fraud operations lead | Theo dõi số alert, năng lực review, fraud amount captured và false positives |
| Data engineer | Theo dõi dữ liệu lỗi, duplicate, lag, freshness và khả năng replay |
| ML engineer | So sánh model với rule-only baseline, kiểm tra drift và truy vết model version |

## Quyết định và chính sách ban đầu

- `APPROVE`: không có hard rule nghiêm trọng và risk score dưới 0,30.
- `REVIEW`: score từ 0,30 đến dưới 0,75 hoặc không thể score an toàn.
- `BLOCK`: hard rule nghiêm trọng hoặc score từ 0,75 trở lên.

Các ngưỡng trên chỉ để xây luồng kỹ thuật. Tuần 7 sẽ chọn lại bằng validation set theo thời gian và năng lực review. Từng quyết định phải lưu transaction ID, rule IDs, feature version, model version, score, thời điểm và processing latency.

## KPI và giả định chi phí

| KPI | Định nghĩa |
|---|---|
| Fraud rate | Số giao dịch được xác nhận fraud / số giao dịch có nhãn |
| Fraud amount captured | Tổng amount của fraud bị BLOCK hoặc REVIEW thành công |
| Missed fraud amount | Tổng amount của fraud được APPROVE |
| False positive rate | Số giao dịch hợp lệ bị BLOCK / số giao dịch hợp lệ có nhãn |
| Review rate | Số giao dịch REVIEW / tổng số quyết định |
| Precision at K | Fraud trong K alert có score cao nhất / K |
| p95 decision latency | Phân vị 95 của thời gian từ ingest đến decision |

Giả định minh họa để so sánh policy, chưa phải số liệu của doanh nghiệp: missed fraud cost bằng 100% amount, manual review cost bằng 5 USD/alert, false block cost bằng 10 USD/giao dịch. Hàm chi phí thử nghiệm: `missed fraud amount + 5 × reviews + 10 × false blocks`. Không công bố các giả định này như kết quả kinh doanh.

## Phạm vi ngoài phiên bản đầu

- Không xử lý tiền thật hay dữ liệu cá nhân thật.
- Không hứa latency ở cấp millisecond; mục tiêu local là p95 dưới 5 giây ở 100 event/giây và phải đo lại.
- Không tự động promote model mới; cần kiểm tra kết quả trước khi đổi champion.
- Không dùng scenario sample có tỷ lệ fraud 10% để báo cáo hiệu quả model.

## Tiêu chí chấp nhận cuối project

- Replay một batch không tạo hai quyết định cho cùng transaction.
- Sự kiện sai schema đi vào đường quarantine/dead-letter với reason code.
- Decision truy vết được về dữ liệu, rule, feature và model version.
- Dashboard lấy KPI từ Gold marts, có `data_as_of` rõ ràng.
- Model được đánh giá theo thời gian và so với rule-only baseline.
