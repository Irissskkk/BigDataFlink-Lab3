
SELECT 'CREATE DATABASE star_db' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'star_db')\gexec

\c star_db;

CREATE TABLE IF NOT EXISTS fact_sales (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    product_id INT NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(100),
    customer_id INT NOT NULL,
    customer_name VARCHAR(200),
    city VARCHAR(100),
    store_id INT,
    sale_amount DECIMAL(10,2),
    sale_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(200),
    city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id INT PRIMARY KEY,
    city VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_transaction_id ON fact_sales(transaction_id);
CREATE INDEX IF NOT EXISTS idx_customer_id ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_product_id ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_store_id ON fact_sales(store_id);
CREATE INDEX IF NOT EXISTS idx_timestamp ON fact_sales(sale_timestamp);

CREATE OR REPLACE VIEW star_schema_view AS
SELECT 
    f.transaction_id,
    f.sale_amount,
    f.sale_timestamp,
    c.customer_name,
    c.city as customer_city,
    p.product_name,
    p.category,
    s.city as store_city
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
LEFT JOIN dim_product p ON f.product_id = p.product_id
LEFT JOIN dim_store s ON f.store_id = s.store_id;

DO $$
BEGIN
    RAISE NOTICE 'База данных star_db и таблица fact_sales готовы!';
END $$;