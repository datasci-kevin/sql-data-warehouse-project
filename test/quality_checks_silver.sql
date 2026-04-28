/*
===============================================================================
DATA QUALITY CHECKS - SILVER LAYER
===============================================================================

Purpose:
    This script contains validation checks for Silver layer tables.

Checks Included:
    - Duplicate checks
    - NULL checks
    - Date validation
    - Data standardization validation
    - Business rule validation

===============================================================================
*/

USE DataWarehouse;
GO

/*
===============================================================================
1. silver.crm_cust_info
===============================================================================
*/

-- Check for duplicate customer IDs
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check for NULL customer IDs
SELECT *
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- Check for unwanted spaces in customer names
SELECT *
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname  != TRIM(cst_lastname);

-- Check standardized marital status values
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- Check standardized gender values
SELECT DISTINCT cst_gender
FROM silver.crm_cust_info;


/*
===============================================================================
2. silver.crm_prd_info
===============================================================================
*/

-- Check for duplicate product IDs
SELECT 
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- Check for NULL product IDs
SELECT *
FROM silver.crm_prd_info
WHERE prd_id IS NULL;

-- Check for NULL product names
SELECT *
FROM silver.crm_prd_info
WHERE prd_nm IS NULL;

-- Check for negative or NULL product costs
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;

-- Check standardized product line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check product date logic
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


/*
===============================================================================
3. silver.crm_sales_details
===============================================================================
*/

-- Check for NULL order numbers
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL;

-- Check invalid sales, quantity, or price values
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;

-- Check sales calculation consistency
-- Expected: sales = quantity * price
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;

-- Check invalid date order
-- Order date should not be after ship date or due date
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check for NULL dates
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt IS NULL
   OR sls_due_dt IS NULL;


/*
===============================================================================
4. silver.erp_cust_az12
===============================================================================
*/

-- Check for NULL customer IDs
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL;

-- Check for invalid birthdates
-- Birthdate should not be in the future
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Check standardized gender values
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Check for unexpected gender values
SELECT *
FROM silver.erp_cust_az12
WHERE gen NOT IN ('Male', 'Female', 'N/A');


/*
===============================================================================
5. silver.erp_loc_a101
===============================================================================
*/

-- Check for NULL customer IDs
SELECT *
FROM silver.erp_loc_a101
WHERE cid IS NULL;

-- Check standardized country values
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

-- Check for unwanted dashes in customer IDs
SELECT *
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%';


/*
===============================================================================
6. silver.erp_px_cat_g1v2
===============================================================================
*/

-- Check for NULL category IDs
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;

-- Check distinct product categories
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2;

-- Check for NULL category fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat IS NULL
   OR subcat IS NULL
   OR maintenance IS NULL;


/*
===============================================================================
7. FINAL ROW COUNT VALIDATION
===============================================================================
*/

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
