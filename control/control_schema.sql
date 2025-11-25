-- ===============================
--  Schema for db.control (MySQL)
--  ETL Process Logging System
-- ===============================

CREATE DATABASE IF NOT EXISTS control
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE control;

-- ===============================
--  Bảng chính để lưu log từng bước
--  của toàn bộ pipeline: extract,
--  staging, transform, load, mart.
-- ===============================
CREATE TABLE IF NOT EXISTS process_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,

    process_name VARCHAR(50) NOT NULL,
    -- Ví dụ: 'extract', 'staging', 'transform', 'load', 'mart'

    status VARCHAR(20) NOT NULL,
    -- Ví dụ: 'running', 'success', 'failed'

    message TEXT NULL,
    -- Thông tin bổ sung, lỗi, mô tả

    start_time DATETIME NOT NULL,
    end_time DATETIME NULL
);

-- ===============================
-- Index để truy vấn nhanh
-- ===============================
CREATE INDEX idx_process_name ON process_logs(process_name);
CREATE INDEX idx_status ON process_logs(status);
