# 🏢 SQL Data Warehouse Project (Bronze → Silver → Gold Architecture)

## 📌 Project Overview

This project demonstrates a complete **Data Warehouse ETL pipeline** using **SQL Server**, implementing the **Medallion Architecture (Bronze, Silver, Gold layers)**.

The pipeline loads raw CRM and ERP data from CSV files into the Bronze layer, cleans and transforms the data in the Silver layer, and creates business-ready Dimension and Fact views in the Gold layer using a **Star Schema**.

This project simulates a real-world enterprise data warehouse used for analytics and reporting.

---

## 🎯 Project Objectives

* Load raw data from CSV files into SQL Server
* Clean, standardize, and transform data
* Remove duplicates and invalid records
* Integrate CRM and ERP systems
* Build Star Schema (Fact and Dimension tables)
* Create analytics-ready datasets
* Follow Medallion Architecture best practices

---

## 🧱 Data Architecture

The project follows the **Medallion Architecture**:

```
CSV Files
   │
   ▼
Bronze Layer  → Raw data (No transformation)
   │
   ▼
Silver Layer  → Cleaned and transformed data
   │
   ▼
Gold Layer    → Business-ready Dimension and Fact views
```

---

## 🥉 Bronze Layer – Raw Data

### 📌 Purpose

Stores raw data exactly as received from source systems.

### 📂 Tables Created

| Table Name               | Description                   |
| ------------------------ | ----------------------------- |
| bronze.crm_cust_info     | CRM customer information      |
| bronze.crm_prd_info      | CRM product information       |
| bronze.crm_sales_details | CRM sales transactions        |
| bronze.erp_cust_az12     | ERP customer demographic data |
| bronze.erp_loc_a101      | ERP customer location data    |
| bronze.erp_px_cat_g1v2   | ERP product category data     |

### ⚙️ Key Features

* Uses `BULK INSERT` to load CSV files
* Uses `TRUNCATE` for full reload
* No transformation applied
* Fast loading performance

### ▶️ Execution

```sql
EXEC bronze.load_bronze;
```

---

## 🥈 Silver Layer – Cleaned Data

### 📌 Purpose

Transforms and cleans Bronze data.

### 🔧 Transformations Performed

* Remove duplicates using ROW_NUMBER()
* Trim spaces
* Standardize gender values
* Standardize marital status
* Fix invalid dates
* Fix incorrect sales values
* Handle NULL values
* Split and extract product category
* Standardize country names

### 📂 Tables Created

| Table Name               | Description              |
| ------------------------ | ------------------------ |
| silver.crm_cust_info     | Clean customer data      |
| silver.crm_prd_info      | Clean product data       |
| silver.crm_sales_details | Clean sales transactions |
| silver.erp_cust_az12     | Clean ERP customer data  |
| silver.erp_loc_a101      | Clean location data      |
| silver.erp_px_cat_g1v2   | Clean category data      |

### ▶️ Execution

```sql
EXEC silver.load_silver;
```

---

## 🥇 Gold Layer – Analytics Layer

This layer contains **Star Schema views** for reporting.

---

## ⭐ Dimension: dim_customer

Contains customer master data.

### Columns

| Column          | Description            |
| --------------- | ---------------------- |
| customer_key    | Surrogate key          |
| customer_id     | Customer ID            |
| customer_number | Customer number        |
| first_name      | First name             |
| last_name       | Last name              |
| country         | Country                |
| marital_status  | Marital status         |
| gender          | Gender                 |
| birthdate       | Birth date             |
| create_date     | Customer creation date |

---

## ⭐ Dimension: dim_products

Contains product information.

### Columns

| Column         | Description        |
| -------------- | ------------------ |
| product_key    | Surrogate key      |
| product_id     | Product ID         |
| product_number | Product number     |
| product_name   | Product name       |
| category       | Category           |
| subcategory    | Subcategory        |
| maintenance    | Maintenance type   |
| product_cost   | Cost               |
| product_line   | Product line       |
| start_date     | Product start date |

---

## ⭐ Fact Table: fact_sales

Contains sales transactions.

### Columns

| Column       | Description  |
| ------------ | ------------ |
| order_number | Order number |
| product_key  | Product key  |
| customer_key | Customer key |
| order_date   | Order date   |
| ship_date    | Ship date    |
| due_date     | Due date     |
| sales_amount | Sales amount |
| quantity     | Quantity     |
| price        | Price        |

---

## 📊 Example Analytics Queries

### Total Sales

```sql
SELECT SUM(sales_amount)
FROM gold.fact_sales;
```

---

### Sales by Country

```sql
SELECT country, SUM(sales_amount)
FROM gold.fact_sales fs
JOIN gold.dim_customer dc
ON fs.customer_key = dc.customer_key
GROUP BY country;
```

---

### Top 10 Customers

```sql
SELECT
dc.first_name,
dc.last_name,
SUM(fs.sales_amount) AS total_sales
FROM gold.fact_sales fs
JOIN gold.dim_customer dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.first_name, dc.last_name
ORDER BY total_sales DESC;
```

---

### Sales by Product Category

```sql
SELECT dp.category,
SUM(fs.sales_amount)
FROM gold.fact_sales fs
JOIN gold.dim_products dp
ON fs.product_key = dp.product_key
GROUP BY dp.category;
```

---

## 🛠 Technologies Used

* SQL Server
* T-SQL
* BULK INSERT
* Stored Procedures
* Views
* Star Schema
* Medallion Architecture

---

## 📂 Project Structure

```
sql-data-warehouse-project/

│
├── bronze/
│   ├── ddl_bronze.sql
│   └── proc_load_bronze.sql
│
├── silver/
│   ├── ddl_silver.sql
│   └── proc_load_silver.sql
│
├── gold/
│   └── ddl_gold.sql
│
└── datasets/
    ├── source_crm/
    └── source_erp/
```

---

## 🚀 How to Run the Project

### Step 1: Create Bronze Tables

```sql
Run ddl_bronze.sql
```

### Step 2: Load Bronze Data

```sql
EXEC bronze.load_bronze;
```

### Step 3: Create Silver Tables

```sql
Run ddl_silver.sql
```

### Step 4: Load Silver Data

```sql
EXEC silver.load_silver;
```

### Step 5: Create Gold Views

```sql
Run ddl_gold.sql
```

---

## 🧠 Skills Demonstrated

* Data Warehouse Design
* ETL Development
* Data Cleaning
* SQL Programming
* Star Schema Modeling
* Medallion Architecture
* Data Transformation
* Data Integration

---

## 📈 Business Value

This warehouse enables:

* Customer analytics
* Sales reporting
* Product performance analysis
* Country-level reporting
* Executive dashboards

---

## 👨‍💻 Author

Ponraj K
Aspiring Data Engineer

---

## ⭐ Conclusion

This project demonstrates real-world Data Engineering skills including ETL pipeline development, data cleaning, warehouse modeling, and analytics layer creation using SQL Server.

---
