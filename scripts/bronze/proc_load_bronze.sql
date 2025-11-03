/*
====================================
Stored procedure : Load Bronze Layer
====================================
*/

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	set @batch_start_time = getdate();
	begin try
		print'================================================';
		print'Loading Bronze Layer';
		print'================================================';

		print'------------------------------------------------';
		print'Loading CRM Tables';
		print'------------------------------------------------';
		
		set @start_time = getdate();
		print'>>Truncatiing Table bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print'>>Inserting Table bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		set @start_time = getdate();
		print'>>Truncatiing Table bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print'>>Inserting Table bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		set @start_time = getdate();
		print'>>Truncatiing Table bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print'>>Inserting Table bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		print'------------------------------------------------';
		print'Loading ERP Tables';
		print'------------------------------------------------';

		set @start_time = getdate();
		print'>>Truncatiing Table bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print'>>Inserting Table bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		set @start_time = getdate();
		print'>>Truncatiing Table bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print'>>Inserting Table bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		set @start_time = getdate();
		print'>>Truncatiing Table bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		print'>>Inserting Table bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		set @batch_end_time = getdate();
		print'=========================================';
		print'Batch Loading Is Completed'
		print'Total Batch Load Duration : '+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds';
		print'=========================================';
	end try
	begin catch
		print'============================================';
		print'Error Occured During Loading Bronze Layer';
		print'Error Meaasge' + error_message();
		print'Error Number' + cast(error_number() as nvarchar);
		print'Error State' + cast(error_state() as nvarchar);
		print'============================================';
	end catch
end
    
/*
    ==============================
    Ececute Stored Procedure
    ==============================
*/
exec bronze.load_bronze;
