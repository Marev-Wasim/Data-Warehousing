use DataWarehouse
go

select * from bronze.crm_prd_info

alter table silver.crm_prd_info
add cat_id NVARCHAR(50);

truncate table silver.crm_prd_info

select * from silver.crm_prd_info

--quality check
select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is NULL

select prd_key
from bronze.crm_prd_info
where prd_key is NULL

select prd_nm
from bronze.crm_prd_info
where prd_nm is NULL or prd_nm != trim (prd_nm)

select *
from bronze.crm_prd_info
where prd_cost is NULL or prd_cost < 0
--210, 211

select *
from bronze.crm_prd_info
where prd_line is NULL
--17 products

select *
from bronze.crm_prd_info
where prd_start_dt is NULL or prd_end_dt is NULL
--11 NULL end dates

select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_px_cat_g1v2

--------------------------------------
insert into silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
select prd_id,
       REPLACE(SUBSTRING(prd_key,1,5),'-','_')          as cat_id,
       SUBSTRING(prd_key,7,LEN(prd_key))                as prd_key,
       TRIM(prd_nm)                                     as prd_nm,
       CASE 
           when prd_cost is NULL or prd_cost < 0 then 0
           else prd_cost
       END                                              as prd_cost,
       CASE
           when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
           when UPPER(TRIM(prd_line)) = 'R' then 'Road'
           when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
           when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
           else 'n/a'
       END                                              as prd_line,
       CAST(prd_start_dt as date)                       as prd_start_dt,
       CAST(LEAD(prd_start_dt) over (
                partition by prd_key order by prd_start_dt) - 1 as date
           )                                            as prd_end_dt
from (
    select *,
           ROW_NUMBER() over (partition by prd_id order by prd_start_dt desc) as rn
    from bronze.crm_prd_info
    where prd_id IS NOT NULL
) as Dedup
where rn = 1;

select *
from silver.crm_prd_info
where prd_cost is NULL or prd_cost < 0

select *
from silver.crm_prd_info
where prd_line is NULL

select *
from silver.crm_prd_info
where prd_start_dt is NULL or prd_end_dt is NULL
--295 NULL end dates