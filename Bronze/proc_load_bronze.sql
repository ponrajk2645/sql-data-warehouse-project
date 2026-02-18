/*
=================================================================================================================
Stored Procedure: bronze.load_bronze
Layer          : Bronze
Purpose        : Load raw data from CSV files into bronze tables

Description:
    This stored procedure performs the initial data ingestion into the Bronze layer.
    It executes the following steps:
        - Truncates existing bronze tables
        - Loads raw data from external CSV files using BULK INSERT
        - Stores data without transformation (raw format)

Source         : External CSV files (CRM and ERP systems)
Target         : bronze schema tables

Parameters     : None

Execution:
    EXEC bronze.load_bronze;

Author         : Ponraj K
Created Date   : 2026
=================================================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @starttime DATETIME;
DECLARE @endtime DATETIME;
DECLARE @batchstarttime DATETIME, @batchendtime DATETIME;
	
		SET @batchstarttime = GETDATE();
		PRINT '===========================================';
		PRINT           'Loading Bronze Layer';
		PRINT '===========================================';

		PRINT '-------------------------------------------';
		PRINT            'Loading CRM Table';
		PRINT '-------------------------------------------';
		SET @starttime = GETDATE();
		PRINT '>> TRUNCATE TABLE : bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> INSERT DATA INTO : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\spach\OneDrive\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endtime = GETDATE();
		PRINT 'LOAD DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @starttime, @endtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'

		SET @starttime = GETDATE();
		PRINT '>> TRUNCATE TABLE : bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> INSERT DATA INTO : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\spach\OneDrive\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endtime = GETDATE();
		PRINT 'LOAD DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @starttime, @endtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'

		SET @starttime = GETDATE();
		PRINT '>> TRUNCATE TABLE : bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> INSERT DATA INTO : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\spach\OneDrive\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endtime = GETDATE();
		PRINT 'LOAD DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @starttime, @endtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'


		PRINT '-------------------------------------------';
		PRINT           'Loading ERP Table';
		PRINT '-------------------------------------------';

		SET @starttime = GETDATE();
		PRINT '>> TRUNCATE TABLE : bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> INSERT DATA INTO : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\spach\OneDrive\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endtime = GETDATE();
		PRINT 'LOAD DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @starttime, @endtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'

		SET @starttime = GETDATE();
		PRINT '>> TRUNCATE TABLE : bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> INSERT DATA INTO : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\spach\OneDrive\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endtime = GETDATE();
		PRINT 'LOAD DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @starttime, @endtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'

		SET @starttime = GETDATE();
		PRINT '>> TRUNCATE TABLE : bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> INSERT DATA INTO : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\spach\OneDrive\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endtime = GETDATE();
		PRINT 'LOAD DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @starttime, @endtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'

		SET @batchendtime = GETDATE();
		PRINT 'BRONZE LAYER LOADIND DURATION:' +' '+ CAST(DATEDIFF(MILLISECOND, @batchstarttime, @batchendtime) as nvarchar) +' '+ 'milliseconds'
		PRINT '----------------'

END;
