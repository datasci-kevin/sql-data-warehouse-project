/*
====================================================================================
SILVER LAYER - TABLE CREATION SCRIPT
====================================================================================

Author: Kevin Garcia
Project: Data Warehouse ETL Pipeline
Layer: Silver (Cleaned & Structured Data)

Description:
    This script creates the Silver layer tables in the data warehouse.
    The Silver layer stores cleaned, standardized, and structured data derived
    from the Bronze layer.

    Enhancements over Bronze:
    - Adds metadata column: dwh_create_date
    - Prepares tables for data cleaning and transformation logic
    - Supports downstream reporting and analytics layers

Notes:
    - Tables are dropped and recreated for development purposes.
    - In production, incremental loads or change tracking would be preferred.

====================================================================================
*/

USE DataWarehouse;
GO

/*
====================================================================================
STEP 1: CREATE SILVER SCHEMA
====================================================================================
*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END;
GO

/*
====================================================================================
STEP 2: DROP EXISTING SILVER TABLES
====================================================================================
*/
DROP TABLE IF EXISTS silver.crm_sales_details;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
GO

/*
====================================================================================
STEP 3: CREATE SILVER TABLES
====================================================================================
*/

-- Create CRM Customer Info Table
CREATE TABLE silver.crm_cust_info
(
    cst_id             INT,             -- Unique customer ID
    cst_key            NVARCHAR(50),    -- Customer business key
    cst_firstname      NVARCHAR(50),    -- Customer first name
    cst_lastname       NVARCHAR(50),    -- Customer last name
    cst_marital_status NVARCHAR(50),    -- Marital status
    cst_gender         NVARCHAR(50),    -- Gender
    cst_create_date    DATE,            -- Source system creation date

    -- Metadata: date and time when the record was loaded into the data warehouse
    dwh_create_date    DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Create CRM Product Info Table
CREATE TABLE silver.crm_prd_info
(
    prd_id          INT,             -- Unique product ID
    prd_key         NVARCHAR(50),    -- Product business key
    prd_nm          NVARCHAR(50),    -- Product name
    prd_cost        INT,             -- Product cost
    prd_line        NVARCHAR(50),    -- Product line/category
    prd_start_dt    DATETIME,        -- Product start date
    prd_end_dt      DATETIME,        -- Product end date

    -- Metadata: date and time when the record was loaded into the data warehouse
    dwh_create_date DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Create CRM Sales Details Table
CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     NVARCHAR(50),    -- Sales order number
    sls_prd_key     NVARCHAR(50),    -- Product key
    sls_cust_id     INT,             -- Customer ID
    sls_order_dt    INT,             -- Order date in raw source format
    sls_ship_dt     INT,             -- Ship date in raw source format
    sls_due_dt      INT,             -- Due date in raw source format
    sls_sales       INT,             -- Sales amount
    sls_quantity    INT,             -- Quantity sold
    sls_price       INT,             -- Price per unit

    -- Metadata: date and time when the record was loaded into the data warehouse
    dwh_create_date DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Create ERP Location Table
CREATE TABLE silver.erp_loc_a101
(
    cid             NVARCHAR(50),    -- Customer ID
    cntry           NVARCHAR(50),    -- Country

    -- Metadata: date and time when the record was loaded into the data warehouse
    dwh_create_date DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Create ERP Customer Table
CREATE TABLE silver.erp_cust_az12
(
    cid             NVARCHAR(50),    -- Customer ID
    bdate           DATE,            -- Birthdate
    gen             NVARCHAR(50),    -- Gender

    -- Metadata: date and time when the record was loaded into the data warehouse
    dwh_create_date DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Create ERP Product Category Table
CREATE TABLE silver.erp_px_cat_g1v2
(
    id              NVARCHAR(50),    -- Product/category ID
    cat             NVARCHAR(50),    -- Category
    subcat          NVARCHAR(50),    -- Subcategory
    maintenance     NVARCHAR(50),    -- Maintenance flag/category

    -- Metadata: date and time when the record was loaded into the data warehouse
    dwh_create_date DATETIME2 DEFAULT SYSDATETIME()
);
GO
