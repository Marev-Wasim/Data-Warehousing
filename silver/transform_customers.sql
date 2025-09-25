use DataWarehouse
go

select * from bronze.crm_cust_info

select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL

insert into silver.crm_cust_info
select cst_id,
       cst_key,
       TRIM(cst_firstname) as cst_firstname,
       TRIM(cst_lastname)  as cst_lastname,
       cst_marital_status,
       cst_gndr,
       cst_create_date
from (
    select *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as rn
    from bronze.crm_cust_info
    where cst_id IS NOT NULL
) as Dedup
where rn = 1;

select * from silver.crm_cust_info

select cst_key, count(*)
from silver.crm_cust_info
group by cst_key
having cst_key is NULL

select cst_firstname, cst_lastname
from silver.crm_cust_info
where cst_firstname is NULL or cst_lastname is NULL

select *
from silver.crm_cust_info
where cst_marital_status is NULL or cst_gndr is NULL or cst_create_date is NULL

update s
set s.cst_gndr = g.gen
from silver.crm_cust_info s
inner join (
    select SUBSTRING(cid, 4, LEN(cid)) as cst_key, gen
    from bronze.erp_cust_az12
) g
on s.cst_key = g.cst_key
where s.cst_gndr IS NULL AND g.gen IS NOT NULL;

update silver.crm_cust_info
set cst_firstname = ISNULL(cst_firstname, 'Unknown'),
    cst_lastname  = ISNULL(cst_lastname,  'Unknown'),
    cst_marital_status = ISNULL(cst_marital_status, 'Unknown'),
	cst_gndr = ISNULL(cst_gndr, 'Unkown')
where cst_firstname is NULL or cst_lastname is NULL or cst_marital_status is NULL or cst_gndr is NULL

select * from silver.crm_cust_info
where cst_id is NULL or cst_key is NULL or cst_firstname is NULL or cst_lastname is NULL
	  or cst_marital_status is NULL or cst_gndr is NULL or cst_create_date is NULL;

