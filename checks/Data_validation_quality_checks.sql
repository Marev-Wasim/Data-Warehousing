use DataWarehouse
go

--1.Duplicates in surrogate keys
--Customers
SELECT customer_key, COUNT(*) 
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

--Products
SELECT product_key, COUNT(*) 
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

--2.Nulls in business keys
SELECT * FROM gold.dim_customers WHERE customer_id IS NULL;
SELECT * FROM gold.dim_products WHERE product_number IS NULL;

--3.Orphan dimension attributes
--Customers without names
SELECT * FROM gold.dim_customers WHERE first_name IS NULL OR last_name IS NULL;

--Products without category
SELECT * FROM gold.dim_products WHERE category IS NULL;

--4.Missing foreign keys (broken joins)
--Orders with products that don’t exist in dim_products
SELECT sd.sls_ord_num, sd.sls_prd_key
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
    ON sd.sls_prd_key = dp.product_number
WHERE dp.product_key IS NULL;

--Orders with customers that don’t exist in dim_customers
SELECT sd.sls_ord_num, sd.sls_cust_id
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
    ON sd.sls_cust_id = dc.customer_id
WHERE dc.customer_key IS NULL;

--6.Date logic consistency
SELECT * 
FROM gold.fact_sales
WHERE order_date > shipping_date
   OR order_date > due_date;

--7.Negative or zero sales
SELECT *
FROM gold.fact_sales
WHERE sales_amount <= 0 
   OR quantity <= 0 
   OR price <= 0;