USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=======================================================================';
        PRINT 'Loading Silver Layer';
        PRINT '=======================================================================';

        /*
        =======================================================================
        CRM TABLES
        =======================================================================
        */

        PRINT '-----------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------------------------------------------------';

        -- Load CRM Customer Info
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gender,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'N/A'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END AS cst_gender,
            cst_create_date
        FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id 
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-----------------------------------------------------------------------';

        -- Load CRM Product Info
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info
        (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(
                    DAY,
                    -1,
                    LEAD(prd_start_dt) OVER (
                        PARTITION BY prd_key
                        ORDER BY prd_start_dt
                    )
                ) AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-----------------------------------------------------------------------';

        -- Load CRM Sales Details
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details
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
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            CASE 
                WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR(8))) != 8 THEN NULL
                ELSE TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112)
            END AS sls_order_dt,

            CASE 
                WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR(8))) != 8 THEN NULL
                ELSE TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112)
            END AS sls_ship_dt,

            CASE 
                WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR(8))) != 8 THEN NULL
                ELSE TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112)
            END AS sls_due_dt,

            CASE 
                WHEN sls_sales IS NULL 
                  OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE 
                WHEN sls_price IS NULL 
                  OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-----------------------------------------------------------------------';

        /*
        =======================================================================
        ERP TABLES
        =======================================================================
        */

        PRINT '-----------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------------------------------------------------';

        -- Load ERP Customer Data
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,

            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,

            CASE 
                WHEN clean_gen IN ('F', 'FEMALE') THEN 'Female'
                WHEN clean_gen IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END AS gen
        FROM bronze.erp_cust_az12
        CROSS APPLY
        (
            SELECT UPPER(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(TRIM(gen), CHAR(9), ''),
                CHAR(10), ''),
                CHAR(13), ''),
                CHAR(160), ''),
                ' ', '')
            ) AS clean_gen
        ) x;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-----------------------------------------------------------------------';

        -- Load ERP Location Data
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN clean_cntry = 'DE' THEN 'Germany'
                WHEN clean_cntry IN ('US', 'USA') THEN 'United States'
                WHEN clean_cntry = '' OR clean_cntry IS NULL THEN 'N/A'
                ELSE clean_cntry
            END AS cntry
        FROM bronze.erp_loc_a101
        CROSS APPLY
        (
            SELECT UPPER(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(TRIM(cntry), CHAR(9), ''),
                CHAR(10), ''),
                CHAR(13), ''),
                CHAR(160), '')
            ) AS clean_cntry
        ) x;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-----------------------------------------------------------------------';

        -- Load ERP Product Category Data
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-----------------------------------------------------------------------';

        /*
        =======================================================================
        FINAL LOAD SUMMARY
        =======================================================================
        */

        SET @batch_end_time = GETDATE();

        PRINT '=======================================================================';
        PRINT 'Silver Layer Loaded Successfully';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '=======================================================================';

        -- Final validation: row count check
        SELECT 'silver.crm_cust_info' AS table_name, COUNT(*) AS row_count FROM silver.crm_cust_info
        UNION ALL
        SELECT 'silver.crm_prd_info', COUNT(*) FROM silver.crm_prd_info
        UNION ALL
        SELECT 'silver.crm_sales_details', COUNT(*) FROM silver.crm_sales_details
        UNION ALL
        SELECT 'silver.erp_cust_az12', COUNT(*) FROM silver.erp_cust_az12
        UNION ALL
        SELECT 'silver.erp_loc_a101', COUNT(*) FROM silver.erp_loc_a101
        UNION ALL
        SELECT 'silver.erp_px_cat_g1v2', COUNT(*) FROM silver.erp_px_cat_g1v2;

    END TRY

    BEGIN CATCH
        PRINT '=======================================================================';
        PRINT 'ERROR OCCURRED DURING SILVER LAYER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '=======================================================================';

        THROW;
    END CATCH
END;
GO

