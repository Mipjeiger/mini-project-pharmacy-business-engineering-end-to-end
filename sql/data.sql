SELECT * FROM raw.pharmacy_sales; -- Ingest data from Pharmacy sales big project
SELECT * FROM features.sales_llm_context; -- Ingest data from Pharmacy sales big project
SELECT * FROM features.sales_feature; -- Ingest data from Pharmacy sales big project
SELECT * FROM features.pharmacy_sales_enhanced; -- Ingest data from Pharmacy sales big project

-- alter table to add new column
ALTER TABLE features.sales_feature
ADD COLUMN rolling_avg_3m_sales NUMERIC;
ADD COLUMN sales_growth_pct NUMERIC;