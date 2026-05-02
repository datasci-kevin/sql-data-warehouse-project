# 🏗️ Data Warehouse ETL Project (Bronze → Silver → Gold)

## 📌 Overview

This project demonstrates the design and implementation of a modern data warehouse using a **layered architecture**:

* **Bronze Layer** → Raw data ingestion
* **Silver Layer** → Data cleaning & transformation
* **Gold Layer** → Business-ready data (star schema for analytics)

The goal of this project is to simulate a real-world ETL pipeline that prepares data for reporting, dashboards, and analysis.

---

## 🧱 Architecture

```
Raw CSV Files → Bronze → Silver → Gold → Analytics
```

### 🔹 Bronze Layer (Raw Data)

* Data loaded directly from CSV files
* No transformations applied
* Stored as-is for traceability

### 🔹 Silver Layer (Cleaned Data)

* Data cleaning (TRIM, standardization, NULL handling)
* Deduplication using window functions
* Data type corrections
* Business rule validation

### 🔹 Gold Layer (Analytics Ready)

* Star schema design:

  * `dim_customers`
  * `dim_products`
  * `fact_sales`
* Surrogate keys created using `ROW_NUMBER()`
* Optimized for reporting and BI tools

---

## 🗂️ Project Structure

```
📁 scripts/
    ├── bronze/
    │   ├── create_tables.sql
    │   └── load_bronze.sql
    │
    ├── silver/
    │   ├── create_tables.sql
    │   └── load_silver.sql
    │
    ├── gold/
    │   └── create_views.sql
    │
📁 procedures/
    ├── bronze.load_bronze.sql
    └── silver.load_silver.sql

📁 tests/
    ├── quality_checks_silver.sql
    └── quality_checks_gold.sql

📁 datasets/
    ├── source_crm/
    └── source_erp/
```

---

## ⚙️ ETL Process

### 1. Bronze Load

* Uses `BULK INSERT`
* Loads raw CSV data into SQL Server
* Includes load time tracking

### 2. Silver Load

* Cleans and standardizes data
* Removes duplicates
* Handles invalid values
* Applies business rules

### 3. Gold Layer

* Builds dimension and fact views
* Enables analytical queries

---

## 🧪 Data Quality Checks

Implemented validations include:

* Referential integrity (Fact ↔ Dimensions)
* Duplicate key detection
* NULL checks
* Data consistency rules:

  * `sales = quantity * price`
* Date validation
* Standardized categorical values

---

## 📊 Example Analysis Queries

```sql
-- Total Sales by Product
SELECT 
    p.product_name,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p 
    ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_sales DESC;
```

```sql
-- Sales by Country
SELECT 
    c.country,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c 
    ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;
```

---

## 🚀 Skills Demonstrated

### Data Engineering

* ETL pipeline design
* Data modeling (star schema)
* Stored procedures
* Data cleaning & transformation
* Performance considerations

### Data Analysis

* Business-ready datasets
* Aggregations & insights
* Dimensional modeling for reporting

---

## 🔥 Key Takeaways

* Built a complete data pipeline from raw data to analytics layer
* Applied real-world data cleaning techniques
* Designed a scalable star schema
* Implemented data validation checks

---

## 📈 Future Improvements

* Add incremental loading (instead of full refresh)
* Integrate Power BI dashboard
* Add indexes for performance
* Automate pipeline with scheduling (SQL Agent / Azure)

---

## 👤 Author

Kevin Garcia
Aspiring Data Analyst / Data Scientist
📍 Houston, TX

