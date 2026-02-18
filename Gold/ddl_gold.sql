/*
=================================================================================================================
Script Name   : ddl_gold_views.sql
Layer         : Gold
Object Type   : Views (Dimension and Fact Views)
Purpose       : Create business-ready analytical views from the Silver layer

Description:
    This script creates Gold layer views representing the final analytical data model
    using a Star Schema design.

    The Gold layer provides clean, enriched, and business-ready datasets optimized
    for reporting, dashboards, and analytics.

    These views include:
        - Dimension views (dim_customer, dim_products)
        - Fact views (fact_sales)

    The views integrate and transform data from multiple Silver layer tables
    to provide a unified and analytics-ready structure.

Source        : silver schema tables
Target        : gold schema views

Data Model    : Star Schema

Usage Example :
    SELECT * FROM gold.dim_customer;
    SELECT * FROM gold.dim_products;
    SELECT * FROM gold.fact_sales;

Author        : Ponraj K
Created Date  : 2026
=================================================================================================================
*/


-- ======================================================================================================
-- Create Dimension: gold.dim_customer
-- ======================================================================================================
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
  DROP VIEW gold.dim_customer;
GO
  
create view gold.dim_customer as
select 
    row_number() over (order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
    	else coalesce(ca.gen,'n/a')
    end as gender,
    ca.bdate as birthdate,
    ci.cst_create_date as create_date
    from silver.crm_cust_info ci
    left join silver.erp_cust_az12 ca
    on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la
    on ci.cst_key = la.cid
GO

-- ======================================================================================================
-- Create Dimension: gold.dim_products
-- ======================================================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
  DROP VIEW gold.dim_products;
GO
  
create view gold.dim_products as
SELECT 
    row_number() over(order by prd_start_dt, prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance,
    pn.prd_cost as product_cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
    FROM silver.crm_prd_info pn
    left join silver.erp_px_cat_g1v2 pc
    on pn.cat_id = pc.id
    where prd_end_dt is null
GO

-- ======================================================================================================
-- Create Fact: gold.fact_sales
-- ======================================================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
  DROP VIEW gold.fact_sales;
GO
  
create view gold.fact_sales as
SELECT
    sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sls_order_dt as order_date,
    sls_ship_dt as ship_date,
    sls_due_dt as due_date,
    sls_sales as sales_amount,
    sls_quantity as quantity,
    sls_price as price
    FROM silver.crm_sales_details sd
    left join gold.dim_customer cu 
    on sd.sls_cust_id = cu.customer_id
    left join gold.dim_products pr
    on sd.sls_prd_key = pr.product_number
GO
