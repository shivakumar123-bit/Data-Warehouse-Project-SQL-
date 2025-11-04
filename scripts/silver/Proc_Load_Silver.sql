/*
Load Silver Layer(Bronze --> Silver)
*/
create or alter procedure silver.load_silver as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	set @batch_start_time = getdate();
	begin try
		print'================================================';
		print'Loading Silver Layer';
		print'================================================';

		print'------------------------------------------------';
		print'Loading CRM Tables';
		print'------------------------------------------------';
		
		set @start_time = getdate();
		print '>> Truncating table silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '>> Inserting table silver.crm_cust_info';
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lasttname,
		case when upper(trim(cst_marital_status)) = 'M' then 'Married'
			when upper(trim(cst_marital_status)) = 'S' then 'Single'
			else 'n/a'
		end as cst_marital_status,
		case when upper(trim(cst_gndr)) = 'M' then 'Male'
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			else 'n/a'
		end as cst_gndr,
		cst_create_date
		from(
			select 
			*,row_number() over(partition by cst_id order by cst_create_date desc) as flag_last 
			from bronze.crm_cust_info
			where cst_id is not null
		)t where flag_last = 1 ;
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		set @start_time = getdate();
		print '>> Truncating table silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>> Inserting table silver.crm_sales_details';
		insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,
			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
			case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
				then sls_quantity * ABS(sls_price)
				else sls_sales
			end as sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <= 0
				then sls_price / nullif(sls_quantity, 0)
				else sls_price
			end  as sls_price
		FROM bronze.crm_sales_details;
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'


		set @start_time = getdate();
		print '>> Truncating table silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> Inserting table silver.crm_prd_info';
		insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
			prd_id,
			replace(substring(prd_key, 1, 5),'-' , '_')as cat_id,
			substring(prd_key, 7, len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost,0) as prd_cost,
			case when upper(trim(prd_line)) = 'M' then 'Mountain'
				when upper(trim(prd_line)) = 'R' then 'Road'
				when upper(trim(prd_line)) = 'S' then 'Other Sales'
				when upper(trim(prd_line)) = 'T' then 'Touring'
				else 'n/a'
			end as prd_line,
			cast(prd_start_dt as date) as prd_start_dt,
			cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		FROM bronze.crm_prd_info;
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'

		print'------------------------------------------------';
		print'Loading ERP Tables';
		print'------------------------------------------------';
		set @start_time = getdate();
		print '>> Truncating table silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print '>> Inserting table silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
			case when CID like 'NAS%' then substring(CID, 4 ,len(CID))
				else CID
			end cid,
			case when BDATE > getdate() then null
				else BDATE
			end as bdate,
			case when upper(trim(GEN)) in ('M','MALE') then 'Male'
				when upper(trim(GEN)) in ('F','FEMALE')then 'Female'
			else 'n/a'
			end as gen
		FROM bronze.erp_cust_az12;
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'


		set @start_time = getdate();
		print '>> Truncating table silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>> Inserting table silver.erp_loc_a101';
		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT 
		replace(CID,'-','') as cid,
		case when trim(CNTRY) = 'DE' then 'Germany'
			when trim(CNTRY) in ('US' , 'USA') then 'United States'
			when trim(CNTRY) = '' or CNTRY is null then 'n/a'
			else trim(CNTRY)
		end as cntry
		FROM bronze.erp_loc_a101;
		set @end_time = getdate();
		print'>> Load Duration :' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------'


		set @start_time = getdate();
		print '>> Truncating table silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '>> inserting table silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT 
		ID as id,
		CAT as cat,
		SUBCAT as subcat,
		MAINTENANCE as maintenance
		FROM bronze.erp_px_cat_g1v2;
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
