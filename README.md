# Data Warehouse Project (Medallion Architecture)

## 📌 Overview
This project implements a **Data Warehouse** following the **Medallion Architecture** (Bronze → Silver → Gold).  
It demonstrates how raw data from CSV sources can be ingested, cleaned, standardized, and transformed into analytics-ready tables for reporting and analysis.

## 🚀 Why This Project is Useful
- Shows a complete **end-to-end data pipeline** using SQL Server.  
- Implements **data quality checks** and cleaning techniques.  
- Creates **dimension and fact tables** for analytics and BI.  
- Provides a reusable framework for other data engineering projects.

## 🛠️ Project Structure
├── bronze/ → Raw data ingestion (Bulk Insert, staging tables)

├── silver/ → Data cleaning, deduplication, enrichment

├── gold/   → Star schema with Dimension & Fact tables

├── checks/ → Data validation & quality checks

└── README.md → Project documentation


## 📂 Layers
- **Bronze**: Raw CSV data loaded into staging tables.  
- **Silver**: Cleaned and standardized data (deduplication, fixing dates, enrichment).  
- **Gold**  : Business-ready star schema with dimension and fact tables.  

## 📝 How to Get Started
1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/data-warehouse-project.git

2. Run the SQL scripts in order:

bronze/ → to create & load raw tables

silver/ → to clean & enrich data

gold/ → to create views (dim_customers, dim_products, fact_sales)

checks/ → to verify data using queries

## ❓Where to Get Help
Review the SQL scripts in the repo.

Open an Issue on GitHub if something is unclear.

## 👩‍💻 Maintainer
This project is maintained by Marev Wasim.

Contributions and suggestions are always welcome!
