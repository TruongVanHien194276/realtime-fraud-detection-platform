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
- Tài liệu ghi license/capacity và giới hạn refresh.

## License và khả năng chia sẻ

Power BI Service cho phép tạo nội dung trong My workspace với license phù hợp, nhưng chia sẻ cho người khác thường cần Power BI Pro, Premium Per User hoặc workspace trên capacity thích hợp. Fabric trial có thời hạn, vì vậy PDF, screenshots, data snapshot và KPI definitions là deliverable bắt buộc để portfolio vẫn xem được sau khi trial kết thúc.

## Tài liệu chính thức

- [Get started creating in the Power BI Service](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-get-started)
- [Edit semantic models in the Power BI Service](https://learn.microsoft.com/en-nz/power-bi/transform-model/service-edit-data-models)
- [Build reports with Direct Lake tables](https://learn.microsoft.com/en-us/fabric/fundamentals/building-reports)
- [Fabric trial capacity](https://learn.microsoft.com/en-us/fabric/fundamentals/fabric-trial)
