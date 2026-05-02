/*
===============================================================================
GOLD LAYER - ANALYTICS VIEWS
===============================================================================

Author: Kevin Garcia
Project: Data Warehouse ETL Pipeline
Layer: Gold (Business-ready Data)

Description:
    This script creates the Gold layer views used for analytics and reporting.

    The Gold layer represents:
    - Cleaned and modeled data
    - Business-friendly naming
    - Star schema structure (Dimensions + Fact table)

Views:
    - dim_products   → Product dimension
    - dim_customers  → Customer dimension
    - fact_sales     → Sales fact table

Notes:
    - Surrogate keys are generated using ROW_NUMBER()
    - Historical records are filtered out where necessary
    - Business logic is applied (e.g., gender prioritization)

===============================================================================
*/

-- ============================================================================
-- VIEW: gold.dim_products
-- ============================================================================

CREATE VIEW gold.dim_products AS 
SELECT
    -- Surrogate key (used for joins in fact table)
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Business identifiers
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,

    -- Descriptive attributes
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,

    -- Product metrics
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,

    -- Date attributes
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 pc 
    ON pn.cat_id = pc.id

-- Keep only current/active products (exclude historical records)
WHERE pn.prd_end_dt IS NULL;
GO


-- ============================================================================
-- VIEW: gold.dim_customers
-- ============================================================================

CREATE VIEW gold.dim_customers AS 
SELECT 
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

    -- Business identifiers
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,

    -- Customer attributes
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender logic:
    -- CRM is considered the primary (trusted) source
    -- If CRM value is 'N/A', fallback to ERP
    CASE 
        WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,

    -- Dates
    ci.cst_create_date AS create_date,
    ca.bdate AS birthday

FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;
GO


-- ============================================================================
-- VIEW: gold.fact_sales
-- ============================================================================

CREATE VIEW gold.fact_sales AS 
SELECT
    -- Order identifiers
    sd.sls_ord_num AS order_number,

    -- Foreign keys to dimensions
    pr.product_key,
    cu.customer_key,

    -- Date attributes
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,

    -- Measures (numeric values for analysis)
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price

FROM silver.crm_sales_details AS sd

-- Join to Product Dimension
LEFT JOIN gold.dim_products AS pr 
    ON sd.sls_prd_key = pr.product_number

-- Join to Customer Dimension
LEFT JOIN gold.dim_customers AS cu 
    ON sd.sls_cust_id = cu.customer_key;
GO
