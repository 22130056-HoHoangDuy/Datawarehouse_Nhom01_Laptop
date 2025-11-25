CREATE TABLE staging_laptop_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    brand VARCHAR(100),
    product_name VARCHAR(255),
    price INT,
    currency VARCHAR(10),
    source VARCHAR(50),
    url TEXT,
    timestamp DATETIME,
    sold_count INT
);
