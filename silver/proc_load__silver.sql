/*
=================================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=================================================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
  Actions Performed:
    -  Truncates Silver tables.
    -  Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
=================================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
	DECLARE @starttime DATETIME, @endtime DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = getdate();
	begin try
		print '============================================';
		print 'Loading Silver Layer';
		print '============================================';

		print ' -------------------------------------------';
		print 'Loading CRM Tables';
		print ' -------------------------------------------';
		SET @starttime = getdate();
		PRINT '>> Truncating Table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into : silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
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
		trim(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END as cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'n/a'
		END as cst_gndr,
		cst_create_date
		from(
			select *, ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info 
			where cst_id is not null
		) t where flag_last = 1;
		SET @endtime = getdate();
		Print '>> load duration : ' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';

		SET @starttime = getdate();
		PRINT '>> Truncating Table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into : silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt,
			prd_end_dt 
		) 	
		SELECT 
		prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
		substring(prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END as prd_line,
		cast(prd_start_dt as date) as prd_start_dt ,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date )as prd_end_dt
		FROM bronze.crm_prd_info;
		SET @endtime = getdate();
		Print '>> load duration : ' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';

		SET @starttime = getdate();
		PRINT '>> Truncating Table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into : silver.crm_sales_details';
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt ,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)	
		select
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
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) 
				 then sls_quantity * abs(sls_price) 
				 else sls_sales
				 end as sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <= 0 
				 then sls_sales / nullif(sls_quantity,0)
				 else sls_price
				 end as sls_price 
		from bronze.crm_sales_details;
		SET @endtime = getdate();
		Print '>> load duration : ' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';


		print '-------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------';
		SET @starttime = getdate();
		PRINT '>> Truncating Table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into : silver.erp_cust_az12';
		insert into silver .erp_cust_az12(
		cid,
		bdate,
		gen
		)
		select  
		case when cid like 'NAS%' THEN substring(cid, 4, len(cid)) 
			 else cid
			 end as cid,
		case when bdate > getdate() then null
			 else bdate
			 end as bdate, 
		case when upper(trim(gen)) in ('F', 'Female') then 'Female'
			 when upper(trim(gen)) in ('M', 'Male') then 'Male'
			 else 'n/a'
			 end gen
		from bronze.erp_cust_az12  ;
		SET @endtime = getdate();
		Print '>> load duration : ' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';

		SET @starttime = getdate();
		PRINT '>> Truncating Table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into : silver.erp_loc_a101';
		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		select 
			replace (cid , '-', '') as cid,
			case when upper(trim(cntry)) in ('USA', 'US') then 'United States'
				 when upper(trim(cntry)) = 'DE' then 'Germany'
				 when upper(trim(cntry)) = '' or cntry is null then 'n/a'
				 else cntry
				 end as cntry
		from bronze.erp_loc_a101;
		SET @endtime = getdate();
		Print '>> load duration : ' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';

		SET @starttime = getdate();
		PRINT '>> Truncating Table : silver .silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into : silver .silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
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
		SET @endtime = getdate();
		Print '>> load duration : ' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';

		SET @endtime = getdate();
		Print 'loading silver layer is complted';
		print '>> Total Load Duration:' + cast(datediff(millisecond, @starttime, @endtime) as varchar) + 'milliseconds';
		print '--------------------------------';
	END TRY
	BEGIN CATCH
	PRINT '================================';
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
	PRINT 'Error message' + error_message();
	PRINT 'Error message' + cast(error_number() as varchar);
	PRINT 'Error message' + cast(error_state() as varchar);
	PRINT '================================';
	END CATCH
END
