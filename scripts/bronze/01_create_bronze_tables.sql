/*
====================================================================================
BRONZE LAYER - TABLE CREATION SCRIPT
====================================================================================

Author: Kevin Garcia
Project: Data Warehouse ETL Pipeline
Layer: Bronze (Raw Data Ingestion)

Description:
    This script initializes the Bronze layer of the data warehouse.
    It ensures the 'bronze' schema exists, drops any existing tables,
    and recreates them to store raw, unprocessed data from source systems.

    The Bronze layer is designed to:
    - Store data as-is from source files (CSV, ERP, CRM)
    - Preserve original structure and values
    - Serve as the foundation for downstream transformations (Silver/Gold)

WARNING:
    - This script will DROP existing Bronze tables if they exist.
    - All data in these tables will be permanently deleted.
    - Use with caution in production environments.

====================================================================================
*/

USE DataWarehouse;
GO

-- Ensure the 'bronze' schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END;
GO

/*
====================================================================================
DROP EXISTING TABLES (RESET STEP)
====================================================================================
Drops all Bronze tables if they exist to allow a clean reload of raw data.
====================================================================================
*/
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
GO

/*
====================================================================================
CREATE TABLE: bronze.crm_cust_info
====================================================================================
Stores raw customer data from CRM system.
====================================================================================
*/
CREATE TABLE bronze.crm_cust_info
(
    cst_id             INT,            -- Unique customer identifier
    cst_key            NVARCHAR(50),   -- Business key from source system
    cst_firstname      NVARCHAR(50),   -- Customer first name
    cst_lastname       NVARCHAR(50),   -- Customer last name
    cst_marital_status NVARCHAR(50),   -- Marital status (raw)
    cst_gender         NVARCHAR(50),   -- Gender (raw, may require standardization)
    cst_create_date    DATE            -- Record creation date
);
GO

/*
====================================================================================
CREATE TABLE: bronze.crm_prd_info
====================================================================================
Stores raw product data from CRM system.
====================================================================================
*/
CREATE TABLE bronze.crm_prd_info
(
    prd_id       INT,            -- Unique product identifier
    prd_key      NVARCHAR(50),   -- Business key
    prd_nm       NVARCHAR(50),   -- Product name
    prd_cost     INT,            -- Product cost
    prd_line     NVARCHAR(50),   -- Product line/category
    prd_start_dt DATETIME,       -- Product start date
    prd_end_dt   DATETIME        -- Product end date
);
GO

/*
====================================================================================
CREATE TABLE: bronze.crm_sales_details
====================================================================================
Stores raw sales transaction data from CRM system.
====================================================================================
*/
CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num  NVARCHAR(50),   -- Order number
    sls_prd_key  NVARCHAR(50),   -- Product key
    sls_cust_id  INT,            -- Customer ID
    sls_order_dt INT,            -- Order date (raw format, requires conversion)
    sls_ship_dt  INT,            -- Ship date (raw format)
    sls_due_dt   INT,            -- Due date (raw format)
    sls_sales    INT,            -- Sales amount
    sls_quantity INT,            -- Quantity sold
    sls_price    INT             -- Price per unit
);
GO

/*
====================================================================================
CREATE TABLE: bronze.erp_loc_a101
====================================================================================
Stores raw location data from ERP system.
====================================================================================
*/
CREATE TABLE bronze.erp_loc_a101
(
    cid   NVARCHAR(50),   -- Customer identifier
    cntry NVARCHAR(50)    -- Country name
);
GO

/*
====================================================================================
CREATE TABLE: bronze.erp_cust_az12
====================================================================================
Stores additional customer attributes from ERP system.
====================================================================================
*/
CREATE TABLE bronze.erp_cust_az12
(
    cid   NVARCHAR(50),   -- Customer identifier
    bdate DATE,           -- Birthdate
    gen   NVARCHAR(50)    -- Gender (may differ from CRM, requires reconciliation)
);
GO

/*
====================================================================================
CREATE TABLE: bronze.erp_px_cat_g1v2
====================================================================================
Stores product category hierarchy from ERP system.
====================================================================================
*/
CREATE TABLE bronze.erp_px_cat_g1v2
(
    id          NVARCHAR(50),   -- Product identifier
    cat         NVARCHAR(50),   -- Category
    subcat      NVARCHAR(50),   -- Subcategory
    maintenance NVARCHAR(50)    -- Maintenance classification
);
GO
