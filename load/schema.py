# schema.py
# FLOW 3.1.1 – Đảm bảo toàn bộ schema tồn tại
from load.load_logger import logger

def ensure_schema(conn):
    with conn.cursor() as cur:
        try:
            logger.info("FLOW 3.1.2: Bắt đầu kiểm tra và tạo các bảng DIM + FACT")

            # FLOW 3.1.3: dim_brand
            logger.info("FLOW 3.1.3: Tạo bảng dim_brand...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_brand (
                    brand_id INT AUTO_INCREMENT PRIMARY KEY,
                    brand_name VARCHAR(100) UNIQUE NOT NULL,
                    INDEX idx_brand_name (brand_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            logger.info("OK: dim_brand")

            # FLOW 3.1.4: dim_source
            logger.info("FLOW 3.1.4: Tạo bảng dim_source...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_source (
                    source_id INT AUTO_INCREMENT PRIMARY KEY,
                    source_name VARCHAR(100) UNIQUE NOT NULL,
                    INDEX idx_source_name (source_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            logger.info("OK: dim_source")

            # FLOW 3.1.5: dim_product
            logger.info("FLOW 3.1.5: Tạo bảng dim_product...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_product (
                    product_id INT AUTO_INCREMENT PRIMARY KEY,
                    product_name VARCHAR(255) UNIQUE NOT NULL,
                    currency VARCHAR(10)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            logger.info("OK: dim_product")

            # FLOW 3.1.6: dim_time
            logger.info("FLOW 3.1.6: Tạo bảng dim_time...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_time (
                    time_id INT AUTO_INCREMENT PRIMARY KEY,
                    crawl_date DATE NOT NULL,
                    crawl_hour INT NOT NULL,
                    UNIQUE KEY uk_time (crawl_date, crawl_hour)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            logger.info("OK: dim_time")

            # FLOW 3.1.7: fact_sales
            logger.info("FLOW 3.1.7: Tạo bảng fact_sales...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_sales (
                    fact_id INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT NOT NULL,
                    brand_id INT NOT NULL,
                    source_id INT NOT NULL,
                    time_id INT NOT NULL,
                    price INT NOT NULL,
                    sold_count INT NULL,
                    `timestamp` DATETIME NULL,
                    KEY idx_product (product_id),
                    KEY idx_brand (brand_id),
                    KEY idx_source (source_id),
                    KEY idx_time (time_id),
                    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
                    CONSTRAINT fk_brand   FOREIGN KEY (brand_id)   REFERENCES dim_brand(brand_id),
                    CONSTRAINT fk_source  FOREIGN KEY (source_id)  REFERENCES dim_source(source_id),
                    CONSTRAINT fk_time    FOREIGN KEY (time_id)    REFERENCES dim_time(time_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            logger.info("OK: fact_sales (bảng cơ bản)")

            # FLOW 3.1.8: đảm bảo UNIQUE KEY cho fact_sales
            logger.info("FLOW 3.1.8: Kiểm tra UNIQUE KEY uk_fact...")
            cur.execute("SHOW INDEX FROM fact_sales WHERE Key_name = 'uk_fact';")
            if not cur.fetchone():
                cur.execute("""
                    ALTER TABLE fact_sales
                    ADD UNIQUE KEY uk_fact (product_id, source_id, time_id);
                """)
                logger.info("Đã tạo UNIQUE KEY uk_fact")
            else:
                logger.info("UNIQUE KEY uk_fact đã tồn tại")

            conn.commit()
            logger.info("FLOW 3.1.9: Toàn bộ schema đã sẵn sàng")

        except Exception as e:
            logger.error(f"LỖI khi tạo schema: {e}", exc_info=True)
            conn.rollback()
            raise