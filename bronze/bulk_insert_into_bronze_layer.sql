use DataWarehouse
go

create procedure bronze.load_bronze as
begin
		truncate table bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'E:\DEPI\Session 34 15.9 project\datasets\source_crm\cust_info.csv'
		with (
		FirstRow= 2,
		FieldTerminator= ',' ,
		TabLock
		);

		truncate table bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'E:\DEPI\Session 34 15.9 project\datasets\source_crm\prd_info.csv'
		with (
		FirstRow= 2,
		FieldTerminator= ',' ,
		TabLock
		);

		truncate table bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'E:\DEPI\Session 34 15.9 project\datasets\source_crm\sales_details.csv'
		with (
		FirstRow= 2,
		FieldTerminator= ',' ,
		TabLock
		);

		truncate table bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'E:\DEPI\Session 34 15.9 project\datasets\source_erp\LOC_A101.csv'
		with (
		FirstRow= 2,
		FieldTerminator= ',' ,
		TabLock
		);

		truncate table bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'E:\DEPI\Session 34 15.9 project\datasets\source_erp\CUST_AZ12.csv'
		with (
		FirstRow= 2,
		FieldTerminator= ',' ,
		TabLock
		);

		truncate table bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'E:\DEPI\Session 34 15.9 project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		FirstRow= 2,
		FieldTerminator= ',' ,
		TabLock
		);

end