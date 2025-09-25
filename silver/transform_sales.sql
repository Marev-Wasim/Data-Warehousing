use DataWarehouse
go

select * from bronze.crm_sales_details

insert into silver.crm_sales_details
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    --dwh_create_date auto-populates
)
select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Order Date
    CASE 
        when sls_order_dt is NULL or sls_order_dt = 0 or LEN(sls_order_dt) != 8 then NULL
        else TRY_CAST(CAST(sls_order_dt as CHAR(8)) as date)
    END as sls_order_dt,

    -- Ship Date
    CASE 
        when sls_ship_dt is NULL or sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then NULL
        else TRY_CAST(CAST(sls_ship_dt as CHAR(8)) as date)
    END as sls_ship_dt,

    -- Due Date
    CASE 
        when sls_due_dt is NULL or sls_due_dt = 0 or LEN(sls_due_dt) != 8 then NULL
        else TRY_CAST(CAST(sls_due_dt as CHAR(8)) as date)
    END as sls_due_dt,

    -- Sales: recalc if missing, invalid, or inconsistent
    CASE 
        when sls_sales is NULL or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) 
            then sls_quantity * ABS(sls_price)
        else sls_sales
    END as sls_sales,

    sls_quantity,

    -- Price: recalc if missing or invalid
    CASE 
        when sls_price is NULL or sls_price <= 0 
            then sls_sales / NULLIF(sls_quantity, 0)
        else sls_price
    END as sls_price

from bronze.crm_sales_details
where sls_ord_num is not NULL;   -- optional, avoid inserting garbage orders

---------------------------------------
--Row Count Check
SELECT 
    (SELECT COUNT(*) FROM bronze.crm_sales_details) AS bronze_count,
    (SELECT COUNT(*) FROM silver.crm_sales_details) AS silver_count;

--Nulls in Key Columns
SELECT COUNT(*) AS null_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL;

SELECT COUNT(*) AS null_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key IS NULL;

SELECT COUNT(*) AS null_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id IS NULL;

---Date Validation
--Orders with invalid timeline
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

--Rows still missing dates
SELECT COUNT(*) AS bad_dates
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL 
   OR sls_ship_dt IS NULL 
   OR sls_due_dt IS NULL;
--19

---Sales / Quantity / Price Consistency
--Sales should equal quantity * price
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * ABS(sls_price);

--No negative or zero quantities
SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity <= 0;

--No negative prices
SELECT *
FROM silver.crm_sales_details
WHERE sls_price < 0;

--Audit Column Check
SELECT TOP 10 sls_ord_num, dwh_create_date
FROM silver.crm_sales_details
ORDER BY dwh_create_date DESC