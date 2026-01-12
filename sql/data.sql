SELECT * FROM raw.pharmacy_sales; -- Ingest data from Pharmacy sales big project
SELECT * FROM features.sales_llm_context; -- Ingest data from Pharmacy sales big project
SELECT * FROM features.sales_feature; -- Ingest data from Pharmacy sales big project
SELECT * FROM features.pharmacy_sales_enhanced; -- Ingest data from Pharmacy sales big project

-- alter table to add new column
ALTER TABLE features.sales_feature
ADD COLUMN rolling_avg_3m_sales NUMERIC;
ADD COLUMN sales_growth_pct NUMERIC;

-- check distinct years in the raw data
SELECT DISTINCT year
FROM raw.pharmacy_sales
ORDER BY year DESC;

SELECT COUNT(*) AS total_rows
FROM features.sales_feature;

SELECT * FROM features.sales_feature LIMIT 10;

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE gold.monthly_sales AS
SELECT
    distributor,
    product_name,
    city,
    channel,
    sub_channel,
    product_class,
    sales_team,
    DATE_TRUNC(sale_date, MONTH) AS sale_month,
    SUM(quantity) AS total_quantity,
    SUM(quantity * price) AS total_sales,
    AVG(price) AS avg_price
FROM silver.sales
GROUP BY 1,2,3,4,5,6,7,8;

CREATE TABLE silver.sales AS
SELECT
    sale_id BIGSERIAL PRIMARY KEY,

    distributor TEXT NOT NULL,
    customer_name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,

    channel TEXT,
    sub_channel TEXT,

    product_name TEXT NOT NULL,
    product_class TEXT,

    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    price DOUBLE PRECISION NOT NULL CHECK (price >= 0),
    sales DOUBLE PRECISION NOT NULL CHECK (sales >= 0),

    month,
    year,
    sales_rep_name,
    manager,
    sales_team
    DATE_TRUNC(sale_date, DAY) AS sale_date

CREATE TABLE silver.pharmacy_sales AS
SELECT * FROM read_parquet()