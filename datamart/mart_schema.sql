CREATE TABLE IF NOT EXISTS mart_brand_summary (
    brand_name VARCHAR(100),
    total_products INT,
    avg_price INT,
    min_price INT,
    max_price INT,
    latest_update DATETIME
);

CREATE TABLE IF NOT EXISTS mart_source_summary (
    source_name VARCHAR(100),
    total_products INT,
    avg_price INT,
    min_price INT,
    max_price INT,
    latest_update DATETIME
);

CREATE TABLE IF NOT EXISTS mart_daily_price_stats (
    crawl_date DATE,
    total_products INT,
    avg_price INT,
    min_price INT,
    max_price INT
);

