# Windows Task Scheduler - ETL Automation Guide

## 1. Mục tiêu
Tự động chạy toàn bộ pipeline:
- Extract → Transform → Load
- Lưu log
- Không cần mở VSCode

## 2. Các bước tạo Task

### Bước 1 — Mở Task Scheduler
- Nhấn **Win + R**
- Gõ: `taskschd.msc`

### Bước 2 — Tạo Basic Task
- Action → Create Basic Task
- Name: `Laptop DataWarehouse Daily`
- Description: Tự động crawl & nạp dữ liệu

### Bước 3 — Chọn Trigger
- Daily
- Thời gian: 08:00 sáng (hoặc tuỳ ý)

### Bước 4 — Action
- Start a Program

### Bước 5 — Chọn file chạy
Program/script:

Add arguments:
automation\pipeline.py
Start in:
E:/Datawarehouse_Nhom01_Laptop/automation/run_daily.bat


### Bước 6 — Finish
→ Kiểm tra log trong `logs\automation.log`

