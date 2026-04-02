# E-Commerce Data Engineering Pipeline 🚀

A sophisticated, production-ready data engineering pipeline for e-commerce analytics built on **Azure Databricks** and **Delta Lake**. This project demonstrates modern data architecture patterns, scalable transformations, and enterprise-grade data workflows.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Key Features](#key-features)
- [Getting Started](#getting-started)
- [Data Flow](#data-flow)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This project implements an **end-to-end data engineering pipeline** following the **Medallion Architecture** (Bronze → Silver → Gold), enabling organizations to transform raw e-commerce data into actionable business intelligence.

The pipeline processes customer transactions, orders, products, and payment data through multiple transformation layers, ultimately delivering clean, aggregated datasets optimized for analytics and reporting on **Power BI** and other BI tools.

**Key Capabilities:**
- Real-time and batch data processing
- Incremental data ingestion with SCD Type 2 support
- Data quality validation and error handling
- Workflow orchestration and scheduling
- Cost-optimized transformations

---

## 🏗️ Architecture

This project follows the **Medallion Architecture**, a best-practice data organization pattern:

```
Raw Data (Sources)
       ↓
    BRONZE 🥉
   (Raw Ingestion)
       ↓
    SILVER 🥈
  (Cleaned & Transformed)
       ↓
     GOLD 🥇
  (Business Metrics)
       ↓
   BI & Analytics
```

### Layer Descriptions

| Layer | Purpose | Data Quality | Use Case |
|-------|---------|--------------|----------|
| **Bronze** | Raw data ingestion with minimal transformation | Raw, unvalidated | Technical debugging, audit trail |
| **Silver** | Cleaned, deduplicated, and conformed data | High quality, validated | Data science, analytics foundations |
| **Gold** | Aggregated, business-ready metrics | Production-ready | Executive dashboards, Power BI reports |

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Cloud Platform** | Microsoft Azure | Scalable infrastructure |
| **Data Warehouse** | Azure Databricks | Unified analytics platform |
| **Distributed Processing** | PySpark | Scalable data transformations |
| **Storage Format** | Delta Lake | ACID transactions, time travel |
| **Data Lake** | Azure Data Lake Storage Gen2 | Cost-effective data storage |
| **Orchestration** | Databricks Jobs | Workflow scheduling & monitoring |
| **Query Language** | SQL | Data exploration & validation |
| **Programming** | Python | ETL logic & transformations |

---

## 📁 Project Structure

```
ecommerce-data-engineering-project/
├── bronze/                          # Raw data ingestion layer
│   └── landing_bronze.py           # Ingest raw data from sources
│
├── silver/                          # Cleaned & transformed data layer
│   ├── silver_d_categories.py      # Product categories dimension
│   ├── silver_d_customers.py       # Customer master data
│   ├── silver_d_products.py        # Product information
│   ├── silver_f_order_items.py     # Order line items facts
│   ├── silver_f_orders.py          # Orders facts
│   ├── silver_f_payments.py        # Payment transactions
│   ├── silver_f_reviews.py         # Customer reviews
│   └── silver_f_shippings.py       # Shipping details
│
├── gold/                            # Business metrics & aggregations
│   ├── gold_daily_sales_aggt.py    # Daily sales aggregations
│   └── gold_fact_sales.py          # Comprehensive sales facts
│
├── infra/                           # Infrastructure & utilities
│   ├── initialize_script.py        # One-time setup script
│   ├── functions.py                # Shared utility functions
│   ├── open_batch.py               # Batch processing start
│   ├── close_batch.py              # Batch processing end
│   └── fail_batch.py               # Error handling & cleanup
│
└── README.md                        # This file
```

---

## ✨ Key Features

### 🔄 Incremental Data Processing
- **Delta Lake Change Data Capture (CDC)** for efficient incremental loads
- **Watermark tracking** to process only new data
- **Minimal data movement** for cost optimization

### 📊 Data Quality Validation
- Schema validation at ingestion
- Null/duplicate checks in silver layer
- Data completeness assertions
- Automated data quality reports

### ⚙️ Workflow Orchestration
- **Databricks Jobs** for scheduled pipeline runs
- **Dependency management** between layers
- **Error handling** and automated retries
- **Monitoring & alerting** on pipeline health

### 📈 Scalable Transformations
- **PySpark for distributed processing** on large datasets
- **Partition strategy** for optimal query performance
- **Multi-threading** for parallel processing
- **Auto-scaling clusters** for cost efficiency

### 📊 Analytics-Ready Datasets
- Pre-aggregated **daily sales metrics**
- **Dimension tables** for efficient joins
- **Fact tables** for comprehensive analysis
- Optimized for **Power BI** and BI tools

### 🔐 Enterprise Features
- **Role-based access control** (RBAC)
- **Data lineage tracking** for compliance
- **Audit logs** for regulatory requirements
- **Backup & disaster recovery** procedures

---

## 🚀 Getting Started

### Prerequisites

- Azure Databricks workspace
- Azure Data Lake Storage Gen2 account
- Python 3.8+
- Databricks CLI (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/bhattabhi338/ecommerce-data-engineering-project.git
   cd ecommerce-data-engineering-project
   ```

2. **Set up Azure resources**
   - Create an Azure Databricks workspace
   - Create an Azure Data Lake Storage Gen2 container
   - Configure access credentials

3. **Configure Database Connection**
   - Update connection strings in `infra/functions.py`
   - Set up Databricks secrets for credentials

4. **Initialize the Pipeline**
   ```python
   # Run in Databricks notebook
   %run ./infra/initialize_script
   ```

5. **Execute the Pipeline**
   - Create Databricks Jobs for each layer (Bronze → Silver → Gold)
   - Schedule jobs for desired frequency (daily, hourly, etc.)

---

## 📊 Data Flow

```
E-Commerce Data Sources
(Orders, Customers, Products, Payments, etc.)
         ↓
    BRONZE Layer
    └─ Raw data landed in Delta tables
         ↓
    SILVER Layer
    ├─ Data cleaning & validation
    ├─ Dimension table creation (Customers, Products, Categories)
    ├─ Fact table creation (Orders, Payments, Shipments, Reviews)
    └─ Data quality checks
         ↓
    GOLD Layer
    ├─ Daily sales aggregations
    ├─ Business metrics calculations
    └─ KPI preparation
         ↓
    BI & Reporting
    └─ Power BI Dashboards
       └─ Executive Reports
```

---

## 📈 Key Metrics Generated

The pipeline calculates:
- **Daily Sales Revenue** by product, category, and region
- **Order Metrics** (count, average value, fulfillment time)
- **Customer Analytics** (acquisition, retention, lifetime value)
- **Payment Performance** (success rates, fraud detection)
- **Shipping Analytics** (delivery times, cost per order)
- **Review Sentiment** (average ratings, trends)

---

## 🔍 Data Assets

### Dimension Tables (Silver Layer)
- `d_customers` - Customer master data with historical tracking
- `d_products` - Product catalog with categories
- `d_categories` - Product category hierarchy

### Fact Tables (Silver Layer)
- `f_orders` - Order transactions with customer and product references
- `f_order_items` - Individual line items per order
- `f_payments` - Payment transaction details
- `f_reviews` - Customer product reviews and ratings
- `f_shippings` - Shipping and delivery information

### Aggregated Tables (Gold Layer)
- `daily_sales_agg` - Summarized daily sales metrics
- `fact_sales` - Comprehensive sales facts with all dimensions

---

## 📚 Documentation

For detailed implementation guides:
- **[Azure Databricks Documentation](https://docs.microsoft.com/azure/databricks/)**
- **[Delta Lake Guide](https://docs.delta.io/)**
- **[PySpark Reference](https://spark.apache.org/docs/latest/api/python/)**

---

## 🤝 Contributing

We welcome contributions! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/EnhancedMetrics`)
3. Commit your changes (`git commit -m 'Add enhanced metrics'`)
4. Push to the branch (`git push origin feature/EnhancedMetrics`)
5. Open a Pull Request

### Contribution Guidelines
- Follow PySpark best practices
- Add unit tests for new transformations
- Update documentation for new features
- Ensure data quality checks pass

---

## 📝 License

This project is licensed under the MIT License — see the LICENSE file for details.

---

## 👨‍💼 Author

**Abhilash Bhatt**  
Data Engineering Portfolio Project

---

## 📞 Support

For questions, issues, or suggestions:
- Open an issue on GitHub
- Contact: [your-email@example.com]

---

<div align="center">

**⭐ If you found this helpful, please consider giving it a star! ⭐**

Built with ❤️ using Azure Databricks & Delta Lake

</div>
