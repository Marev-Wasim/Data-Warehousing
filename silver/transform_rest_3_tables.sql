use DataWarehouse
go

alter table silver.erp_cust_az12
add dwh_create_date DATETIME2 DEFAULT GETDATE();

insert into silver.erp_cust_az12 (cid, bdate,gen)
select
	CASE
		when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
		else cid
	END as cid,

	CASE
		when bdate > GETDATE() then NULL
		else bdate
	END as bdate, -- Set future birthdates to NULL
	CASE
		when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
		when UPPER(TRIM(gen)) in ('M', 'MALE')   then 'Male'
		else 'n/a'
	END as gen -- Normalize gender values and handle unknown cases
from bronze.erp_cust_az12;

select * from silver.erp_cust_az12

----------------------------------------------------

alter table silver.erp_loc_a101
add dwh_create_date DATETIME2 DEFAULT GETDATE();


select * from bronze.erp_loc_a101
select * from silver.erp_loc_a101

insert into silver.erp_loc_a101 (cid, cntry)
select
    REPLACE(cid, '-', '') as cid, 
CASE
	when TRIM(cntry) = 'DE'			  then 'Germany'
    when TRIM(cntry) in ('US', 'USA') then 'United States'
    when cntry is NULL or TRIM(cntry) = '' THEN 'n/a'
	else TRIM(cntry)
END as cntry			-- Normalize and Handle missing or blank country codes
FROM bronze.erp_loc_a101;

-----------------------------------------------------------

alter table silver.erp_px_cat_g1v2
add dwh_create_date DATETIME2 DEFAULT GETDATE();

insert into silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
)
select
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_px_cat_g1v2;