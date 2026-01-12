SELECT * FROM glob('s3://silver/')
ORDER BY last_modified DESC
LIMIT 1;

-- Load from MinIO Silver new data batch
CREATE OR REPLACE TABLE silver.pharmacy_sales AS
SELECT *
FROM read_parquet('s3://silver/pharmacy_sales_20260112_210850_*.parquet'); -- please check about new date pattern updated for the next import batch one

-- Validate the data loaded
SELECT
    COUNT(*) AS total_rows,
    MIN(year),
    MAX(year)
FROM silver.pharmacy_sales;

SELECT * FROM silver.pharmacy_sales LIMIT 10;