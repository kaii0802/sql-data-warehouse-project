/*
================================================================================
Quality Checks for Silver Layer
================================================================================
Script Purpose:
    This script performs various checks for data consistency, accuracy,
    and standardization across the 'silver' schema. It includes checks for:
    - Nulls or duplicates in primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
================================================================================
*/

-- =============================================================================
-- Check: silver.crm_cust_info
-- =============================================================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces in names
-- Expectation: No Results
SELECT 
    cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname  != TRIM(cst_lastname);

-- Check for invalid gender values
SELECT DISTINCT 
cst_gndr 
FROM silver.crm_cust_info;

-- Check for invalid marital status values
SELECT DISTINCT 
    cst_material_status 
FROM silver.crm_cust_info;

-- =============================================================================
-- Check: silver.crm_prd_info
-- =============================================================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces in product names
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Nulls or Negative Numbers in cost
SELECT 
    prd_id, prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Check for invalid product date ranges
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- =============================================================================
-- Check: silver.crm_sales_details
-- =============================================================================

-- Check for Invalid Date Formats in Bronze Layer
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
SELECT DISTINCT
    sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- =============================================================================
-- Check: silver.erp_cust_az12
-- =============================================================================

-- Identify Out-of-Range Birthdates
SELECT DISTINCT 
    bdate, gen
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standardization: Gender
SELECT DISTINCT 
    gen AS gen_raw,
    CASE 
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE')   THEN 'Male'
        ELSE 'n/a'
    END AS gen_clean
FROM silver.erp_cust_az12;

-- =============================================================================
-- Check: silver.erp_loc_a101
-- =============================================================================

-- Check for standardization of country codes
SELECT DISTINCT 
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- =============================================================================
-- Check: silver.erp_px_cat_g1v2
-- =============================================================================

-- Check for unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization: Maintenance
SELECT DISTINCT 
    REPLACE(maintenance, CHAR(13), '') AS maintenance_clean
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;

