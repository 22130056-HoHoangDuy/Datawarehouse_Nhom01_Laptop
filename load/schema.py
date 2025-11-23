# schema.py
def ensure_schema(conn):
    """
    Tạo DIM + FACT theo đúng cấu trúc gốc của nhóm anh Duy.
    """
    with conn.cursor() as cur:
        # DIM BRAND
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_brand (
                brand_id INT AUTO_INCREMENT PRIMARY KEY,
                brand_name VARCHAR(100) UNIQUE,
                INDEX(brand_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # DIM SOURCE
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_source (
                source_id INT AUTO_INCREMENT PRIMARY KEY,
                source_name VARCHAR(100) UNIQUE,
                INDEX(source_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # DIM PRODUCT
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(255) UNIQUE,
                currency VARCHAR(10)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # DIM TIME
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id INT AUTO_INCREMENT PRIMARY KEY,
                crawl_date DATE NOT NULL,
                crawl_hour INT NOT NULL,
                UNIQUE KEY uk_time (crawl_date, crawl_hour)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # FACT SALES
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                fact_id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT,
                brand_id INT,
                source_id INT,
                time_id INT,
                price INT,
                sold_count INT NULL,
                `timestamp` DATETIME NULL,

                KEY(product_id),
                KEY(brand_id),
                KEY(source_id),
                KEY(time_id),

                CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
                CONSTRAINT fk_brand   FOREIGN KEY (brand_id)   REFERENCES dim_brand(brand_id),
                CONSTRAINT fk_source  FOREIGN KEY (source_id)  REFERENCES dim_source(source_id),
                CONSTRAINT fk_time    FOREIGN KEY (time_id)    REFERENCES dim_time(time_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # UNIQUE GRAIN (product + source + time)
        cur.execute("""
            ALTER TABLE fact_sales 
            ADD UNIQUE KEY IF NOT EXISTS uk_fact (product_id, source_id, time_id);
        """)

    conn.commit()
