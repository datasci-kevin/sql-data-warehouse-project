/*
===============================================================================
DATA QUALITY CHECKS - GOLD LAYER
===============================================================================

Purpose:
    Validate the Gold layer views used for reporting and analytics.

Checks Included:
    - Referential integrity between fact and dimension views
    - Duplicate surrogate key checks
    - NULL key checks
    - Business measure validation
    - Date logic validation

Expected Result:
    Most checks should return zero rows.
===============================================================================
*/

USE DataWarehouse;
GO

/*
===============================================================================
1. Referential Integrity Check
===============================================================================
Purpose:
    Ensure every sales record in the fact table connects to a valid customer
    and product dimension record.

Expected Result:
    This query should return zero rows.
===============================================================================
*/

SELECT *
FROM gold.fact_sales AS f 
LEFT JOIN gold.dim_customers AS c 
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL 
   OR p.product_key IS NULL;
GO


/*
===============================================================================
2. Duplicate Customer Key Check
===============================================================================
Purpose:
    Ensure each customer_key in the customer dimension is unique.

Expected Result:
    This query should return zero rows.
===============================================================================
*/

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers 
GROUP BY customer_key
HAVING COUNT(*) > 1;
GO


/*
===============================================================================
3. Duplicate Product Key Check
===============================================================================
Purpose:
    Ensure each product_key in the product dimension is unique.

Expected Result:
    This query should return zero rows.
===============================================================================
*/

SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products 
GROUP BY product_key
HAVING COUNT(*) > 1;
GO


/*
===============================================================================
4. NULL Key Check - Fact Table
===============================================================================
Purpose:
    Ensure fact table records have valid foreign keys.

Expected Result:
    This query should return zero rows.
===============================================================================
*/

SELECT *
FROM gold.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL;
GO


/*
===============================================================================
5. NULL Primary Key Check - Dimensions
===============================================================================
Purpose:
    Ensure dimension views do not contain NULL surrogate keys.

Expected Result:
    These queries should return zero rows.
===============================================================================
*/

SELECT *
FROM gold.dim_customers
WHERE customer_key IS NULL;
GO

SELECT *
FROM gold.dim_products
WHERE product_key IS NULL;
GO


/*
===============================================================================
6. Sales Amount Validation
===============================================================================
Purpose:
    Validate the business rule:
        sales_amount = quantity * price

Expected Result:
    This query should return zero rows.
===============================================================================
*/

SELECT *
FROM gold.fact_sales
WHERE sales_amount != quantity * price
   OR sales_amount IS NULL
   OR quantity IS NULL
   OR price IS NULL
   OR sales_amount <= 0
   OR quantity <= 0
   OR price <= 0;
GO


/*
===============================================================================
7. Date Logic Validation
===============================================================================
Purpose:
    Ensure order dates follow a valid business sequence:
        order_date <= shipping_date
        order_date <= due_date

Expected Result:
    This query should return zero rows.
===============================================================================
*/

SELECT *
FROM gold.fact_sales
WHERE order_date > shipping_date
   OR order_date > due_date;
GO


/*
===============================================================================
8. Dimension Value Standardization Checks
===============================================================================
Purpose:
    Confirm that cleaned categorical values are standardized.

Expected Result:
    Review returned values manually.
===============================================================================
*/

SELECT DISTINCT gender
FROM gold.dim_customers
ORDER BY gender;
GO

SELECT DISTINCT marital_status
FROM gold.dim_customers
ORDER BY marital_status;
GO

SELECT DISTINCT country
FROM gold.dim_customers
ORDER BY country;
GO

SELECT DISTINCT product_line
FROM gold.dim_products
ORDER BY product_line;
GO


/*
===============================================================================
9. Duplicate Business Key Checks
===============================================================================
Purpose:
    Ensure business identifiers are not duplicated in dimension views.

Expected Result:
    These queries should return zero rows.
===============================================================================
*/

SELECT 
    customer_id,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
GO

SELECT 
    product_id,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_id
HAVING COUNT(*) > 1;
GO


/*
===============================================================================
10. Final Row Count Validation
===============================================================================
Purpose:
    Provide row counts for Gold layer views.

Expected Result:
    Used for general validation and documentation.
===============================================================================
*/

SELECT 'gold.dim_customers' AS view_name, COUNT(*) AS row_count FROM gold.dim_customers
UNION ALL
SELECT 'gold.dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'gold.fact_sales', COUNT(*) FROM gold.fact_sales;
GO
