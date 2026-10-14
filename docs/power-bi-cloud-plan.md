# Kế hoạch Power BI Cloud

Dashboard portfolio được xây hoàn toàn trong Power BI Service hoặc Microsoft Fabric qua trình duyệt. Power BI Desktop và Windows VM không nằm trong kiến trúc dự án.

## Luồng chính với Microsoft Fabric

Luồng này được dùng khi tài khoản có Fabric trial hoặc capacity phù hợp:

```text
PostgreSQL Gold marts
        |
        v
Versioned Parquet export
        |
        v
Fabric Lakehouse / OneLake
        |
        v
Semantic model
        |
        v
Power BI web report
```

Các bảng Gold được xuất từ local phải nhỏ, đã aggregate và mask identifier. Một manifest đi kèm ghi snapshot ID, thời gian xuất, row count, schema hash và dbt build/run ID.

Trong Fabric:

1. Tạo workspace riêng cho project.
2. Tạo Lakehouse và nạp các Gold tables.
3. Kiểm tra data types, date table và relationships.
4. Tạo hoặc chỉnh semantic model trong web editor.
5. Tạo DAX measures từ KPI catalog.
6. Xây report trong browser.
7. Export PDF và lưu screenshots để portfolio không phụ thuộc trạng thái trial.

Direct Lake được dùng nếu capacity/workspace hỗ trợ. Không sử dụng Copilot làm dependency của dashboard.

## Luồng fallback với Power BI Service

Nếu tài khoản không có Fabric Lakehouse, dự án vẫn dùng Power BI Cloud:

1. Export Gold marts thành một workbook Excel có nhiều bảng hoặc các file Excel theo subject area.
2. Upload vào My workspace tại Power BI Service.
3. Tạo semantic model từ file đã upload.
4. Chỉnh relationships, DAX measures và row-level security trong web editor nếu license hỗ trợ.
5. Tạo report và dashboard trong browser.

Luồng này phù hợp cho portfolio nhưng không được gọi là real-time BI. Snapshot phải hiển thị rõ `data_as_of`.

## Vì sao không kết nối local PostgreSQL trực tiếp

Power BI Cloud không thể tự truy cập PostgreSQL đang nằm trong laptop phía sau mạng local. Dùng gateway hoặc expose database ra Internet sẽ tăng rủi ro, phụ thuộc nền tảng và chi phí vận hành. Phiên bản đầu dùng curated snapshot hoặc OneLake để giữ phạm vi an toàn và có thể tái tạo.

## Data contract cho BI snapshot

Mỗi snapshot gồm:

- `fact_fraud_daily`
- `fact_fraud_decisions`
- `fact_alert_performance`
- `fact_pipeline_health`
- `dim_date`
- `dim_transaction_type`
- `dim_rule`
- `dim_model_version`

Các trường định danh người dùng phải được hash hoặc mask. Không upload raw account name, device ID, IP hoặc event payload lên workspace portfolio.

## Deliverables

- Script export snapshot.
- Snapshot manifest.
- Semantic model relationship map.
- DAX measure catalog.
- Ba report pages.
- PDF export và screenshots.

## Snapshot Week 6 và dựng lại report

`make bi-build && make bi-export` tạo `data/exports/bi/<snapshot-id>/` gồm tám bảng Parquet/JSON và `manifest.json`. Chỉ dùng một `run_id` cho quyết định; Gold daily bao phủ 100k giao dịch nên không dùng Gold count làm mẫu số approval rate. `data/exports/` bị Git ignore vì có dữ liệu giao dịch ở mức hàng. Key HMAC được tạo một lần ở `data/tmp/bi_mask_key.hex` (mode 0600), không upload hoặc commit. Giữ key này nếu cần so khớp customer giữa các snapshot. Không có raw `customer_id` trong bảng xuất.

Workbook fallback có thể dựng từ snapshot bằng `docker compose --profile bi run --rm bi python scripts/build_powerbi_workbook.py data/exports/bi/<snapshot-id>`. Script kiểm tra số hàng khi mở lại file. Do bộ Artifact Tool không khả dụng trong môi trường tác vụ này, builder dùng `openpyxl` để tạo Excel Tables cho Power BI Service. Key và Parquet/JSON row-level vẫn ở thư mục ignored. Một workbook curated đã mask và manifest của snapshot kiểm chứng được lưu tại `dashboards/powerbi/exports/` để recruiter có thể phục dựng semantic model; đây là nguồn dữ liệu, **không phải Power BI report**.

Local validation: `make bi-preview`, mở `http://127.0.0.1:8501`. Đây là bản kiểm tra query/KPI trên Mac, không phải Power BI Cloud report.

### Semantic model (web editor)

- `dim_date[event_date]` 1:* tới `fact_fraud_daily[event_date]`, `fact_alert_performance[event_date]`, `fact_fraud_decisions[event_date]`, `fact_customer_timeline[event_date]`.
- `dim_transaction_type[transaction_type]` 1:* tới bốn fact tương ứng.
- `fact_fraud_decisions[transaction_id]` 1:* tới `fact_customer_timeline[transaction_id]` chỉ khi key là duy nhất ở hai bảng; nếu không, dùng drill-through filter `customer_key` mà không tạo relationship này.
- Filter direction single từ dimension tới fact. Không tạo relationship fact-to-fact giữa Gold và decision populations.
- `dim_rule` là reference list; `reason_codes` là JSON text, chưa phải bridge table. Không tạo relationship giả.
- `fact_pipeline_health` là một hàng snapshot; hiển thị `snapshot_at` và replay event max, không đưa vào date relationship.

Các measure DAX nằm tại `dashboards/powerbi/measures.dax`. Tạo từng measure trong web editor. Đặt date type cho `event_date`; cột timestamp trong workbook là UTC không kèm timezone. `docs/kpi-catalog.md` định nghĩa mẫu số và giới hạn từng KPI.

### Layout ba trang cần tạo trong Power BI Service

1. **Fraud Command Center**: slicer date/type; cards Gold Transactions, Transaction Value, Scored Decisions, Alerts, Alert Precision; stacked column decision mix by day; line transaction value by day; table alert outcomes. Chú thích hai populations khác nhau và simulated amounts.
2. **Investigation Detail**: alert table `transaction_id`, `customer_key`, event time, decision, score, reason, label; drill-through field `customer_key`; customer timeline table sorted event time. Không hiển thị raw ID. Label chỉ retrospective.
3. **Platform & Data Quality**: Gold balance mismatches by day, P50/P95 decision processing latency, snapshot time, selected run and row counts. Ghi rõ historical replay snapshot, không gọi là live data freshness.

Khi có workspace và license phù hợp, ưu tiên upload các Parquet vào Fabric Lakehouse. Nếu không có Fabric, upload workbook fallback vào Power BI Service. Tạo model/report từ các bảng curated, sau đó export PDF, chụp ba trang và lưu ở `dashboards/powerbi/exports/` (ảnh/PDF đã kiểm tra riêng trước khi commit). Chưa có export cloud nào trong repository thì chưa thể gọi Week 6 hoàn thành.

### Kết quả local ngày 2026-10-14

Snapshot `20261014T164935Z-week5-smoke-v1` có 100.000 Gold transactions, 999 scored decisions, 151 dòng Gold daily/type, 999 customer timeline rows và 31 ngày. Từ delayed labels: TP=2, FP=696, FN=0, TN=301. Precision xấp xỉ 0,29%, recall 100%; sample chỉ có hai fraud cases nên không diễn giải là policy tốt. Local validation ba tab chạy không lỗi trong Streamlit AppTest. P50 ingest-to-decision khoảng 40,5 triệu ms vì replay bị chờ trước khi worker chạy, không phải latency serving thực tế.
- Tài liệu ghi license/capacity và giới hạn refresh.

## License và khả năng chia sẻ

Power BI Service cho phép tạo nội dung trong My workspace với license phù hợp, nhưng chia sẻ cho người khác thường cần Power BI Pro, Premium Per User hoặc workspace trên capacity thích hợp. Fabric trial có thời hạn, vì vậy PDF, screenshots, data snapshot và KPI definitions là deliverable bắt buộc để portfolio vẫn xem được sau khi trial kết thúc.

## Tài liệu chính thức

- [Get started creating in the Power BI Service](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-get-started)
- [Edit semantic models in the Power BI Service](https://learn.microsoft.com/en-nz/power-bi/transform-model/service-edit-data-models)
- [Build reports with Direct Lake tables](https://learn.microsoft.com/en-us/fabric/fundamentals/building-reports)
- [Fabric trial capacity](https://learn.microsoft.com/en-us/fabric/fundamentals/fabric-trial)
