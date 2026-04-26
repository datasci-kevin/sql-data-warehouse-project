/*
====================================================================================
BRONZE LAYER - DATA LOADING (BULK INSERT)
====================================================================================

Author: Kevin Garcia
Project: Data Warehouse ETL Pipeline
Layer: Bronze (Raw Data Ingestion)

Description:
    This script performs the data loading process into the Bronze layer.
    It first truncates existing tables to ensure a clean reload, then
    loads raw data from CSV files using BULK INSERT.

    The data sources include:
    - CRM system (customers, products, sales)
    - ERP system (customer info, locations, product categories)

    This process represents the "Extract & Load" phase of ETL.

Key Notes:
    - Data is loaded in its raw format (no transformations applied)
    - CSV files are stored locally for ingestion
    - FIRSTROW = 2 skips header rows
    - ROWTERMINATOR ensures proper row separation

====================================================================================
*/

USE DataWarehouse;
GO

/*
====================================================================================
STEP 1: RESET BRONZE TABLES
====================================================================================
Removes all existing data while preserving table structure.
This allows for repeatable and consistent data loads.
====================================================================================
*/
TRUNCATE TABLE bronze.crm_cust_info;
TRUNCATE TABLE bronze.crm_prd_info;
TRUNCATE TABLE bronze.crm_sales_details;
TRUNCATE TABLE bronze.erp_cust_az12;
TRUNCATE TABLE bronze.erp_loc_a101;
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
GO

/*
====================================================================================
STEP 2: LOAD CRM DATA (SOURCE: CRM SYSTEM)
====================================================================================
Loads raw CRM data into Bronze tables.
====================================================================================
*/

-- Load Customer Data
BULK INSERT bronze.crm_cust_info
FROM 'C:\SQLDataWarehouse\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,                  -- Skip header row
    FIELDTERMINATOR = ',',         -- Column delimiter
    ROWTERMINATOR = '0x0a',        -- Line feed (newline)
    TABLOCK                        -- Improves performance for bulk operations
);
GO

-- Load Product Data
BULK INSERT bronze.crm_prd_info
FROM 'C:\SQLDataWarehouse\datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

-- Load Sales Data
BULK INSERT bronze.crm_sales_details
FROM 'C:\SQLDataWarehouse\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

/*
====================================================================================
STEP 3: LOAD ERP DATA (SOURCE: ERP SYSTEM)
====================================================================================
Loads raw ERP data into Bronze tables.
====================================================================================
*/

-- Load ERP Customer Data
BULK INSERT bronze.erp_cust_az12
FROM 'C:\SQLDataWarehouse\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

-- Load ERP Location Data
BULK INSERT bronze.erp_loc_a101
FROM 'C:\SQLDataWarehouse\datasets\source_erp\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

-- Load ERP Product Category Data
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\SQLDataWarehouse\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

/*
====================================================================================
STEP 4: DATA VALIDATION (ROW COUNT CHECK)
====================================================================================
Quick validation step to confirm data was loaded successfully.
Useful for debugging and ensuring completeness of ingestion.
====================================================================================
*/

SELECT 'bronze.crm_cust_info' AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'bronze.crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'bronze.crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'bronze.erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
UNION ALL
SELECT 'bronze.erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
