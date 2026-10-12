# Cấu hình môi trường phát triển

Kiến trúc dự án được điều chỉnh cho MacBook Pro Apple M3 có 8 GB RAM, trong đó Docker hiện được cấp khoảng 4 GB. Mục tiêu là chạy từng lát cắt của hệ thống ổn định thay vì chạy toàn bộ production-like stack cùng lúc.

## Cấu hình quan sát ngày 10 tháng 10 năm 2026

| Thành phần | Giá trị |
|---|---|
| Model | MacBook Pro Mac15,3 |
| Chip | Apple M3 ARM64 |
| CPU | 8 cores |
| RAM | 8 GB |
| Docker memory | khoảng 4 GB |
| Dung lượng còn trống | khoảng 171 GB |
| Host Python | 3.14.4 |
| Host Java | OpenJDK 25 |

## Ràng buộc triển khai

- Runtime Spark/Airflow chạy trong container thay vì dùng Python và Java trên host.
- Tuần 1 pin DuckDB 1.5.6, PyArrow 23.0.1, Pydantic 2.14.1, pytest 9.1.1 và Ruff 0.17.0 trong `pyproject.toml`; các bản này đã build trên ARM64 container.
- Ưu tiên image có hỗ trợ `linux/arm64`; tránh ép `amd64` vì emulation tốn CPU và RAM.
- Spark chạy local mode với 2 cores trong developer loop.
- Chỉ bật các Docker Compose profiles đang cần.
- Dataset raw, checkpoint, model artifacts và lakehouse tables không được commit vào Git.
- Dự án dùng Power BI Service/Fabric trong trình duyệt, không dùng Power BI Desktop hoặc Windows VM. Gold marts được đưa lên cloud dưới dạng Fabric Lakehouse/OneLake tables hoặc một Excel snapshot có version.

## Ngưỡng dừng để bảo vệ trải nghiệm máy

Dừng benchmark hoặc giảm tốc độ khi xảy ra một trong các điều kiện:

- macOS bắt đầu swap kéo dài hoặc UI phản hồi kém.
- Container bị OOM kill.
- Spark garbage collection chiếm phần lớn thời gian chạy.
- Disk free giảm xuống dưới 50 GB.
- p95 latency tăng liên tục trong khi Kafka lag không hồi phục.

Mọi benchmark portfolio phải ghi kèm dataset size, event rate, số cores và memory limit.
