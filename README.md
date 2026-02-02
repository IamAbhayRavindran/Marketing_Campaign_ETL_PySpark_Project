# Marketing Campaign ETL Pipeline (PySpark)

An end-to-end **ETL pipeline built with PySpark** to process, clean, transform, and analyze marketing campaign data using a **Medallion Architecture (Bronze → Silver → Gold)** with **incremental loading**, **data validation**, and **SQL-based analytics**.

---

## Project Overview

This project ingests raw marketing campaign CSV files, processes them through multiple structured layers, and generates analytical outputs suitable for reporting and business insights.

### Key Features
- PySpark-based scalable ETL
- Medallion architecture (Bronze, Silver, Gold)
- Incremental data processing
- Business-rule filtering
- SQL-based metrics generation
- Robust logging (JSON + console)
- Data quality validation

---

## Architecture
RAW (CSV)
↓
BRONZE (Parquet)
↓
SILVER CLEANED (Data Cleansing)
↓
SILVER FILTER FINAL (Business Rules)
↓
GOLD (Aggregations & SQL Metrics)


---

## Project Structure

Marketing_Campaign_ETL_Pipeline/
│
├── aggregation/ # Gold layer aggregations
│ └── gold_agg.py
│
├── config/
│ ├── env.py # PySpark & Hadoop environment setup
│ ├── path.py # Centralized path management
│ └── spark_session.py # Spark session configuration
│
├── data/
│ ├── raw/ # Raw CSV input files
│ ├── bronze/ # Bronze Parquet data
│ ├── silver/
│ │ └── silver_cleaned/
│ │ └── silver_filter_final/
│ ├── gold/
│ │ └──gold_agg/
│ │      └── Campaign Summary
│ │      └── Channel Summary
│ │     └──  Daily Summary
│   └── sql_metrics/
│
├── ingestion/
│ └── bronze_ingestion.py
│
├── processed/
│ ├── cleaning/
│ │ └── silver_cleaning.py
│ └── filter_final/
│ └── silver_filter_final.py
│
├── transformations/
│ └── sql_metrics.py
│
├── profiling/
│ └── data_profiling.py
│
├── validation/
│ └── data_validation.py
│
├── incremental/
│ └── incremental_load.py
│
├── data_read/
│ └── file_read.py
│
├── schemas/
│ └── schemas.py
│
├── utils/
│ └── logger.py
│
├── logs/ # JSON & console logs
├── new_file/ # Marketing Campaign Dataset File (Upcoming also)
│
├── main.py # Master pipeline runner
├── requirements.txt
└── README.md



---

## Technologies Used

- Python 3.10
- Apache Spark (PySpark)
- Hadoop (Windows setup)
- Parquet
- Spark SQL
- Logging (JSON structured logs)

---

## How to Run the Pipeline

### 1️ Create Virtual Environment
```bash
python -m venv venv
venv\Scripts\activate   # Windows


### 2 Install Dependencies
pip install -r requirements.txt


### 3 Configure Paths

Update paths if required:

config/env.py

config/path.py

Ensure:

Python path is correct

Hadoop bin directory is available


### 4 Run Full Pipeline
python main.py



### 5 Incremental Processing

The pipeline supports incremental loading:

Detects new records using timestamps

Prevents duplicate Campaign_ID

Appends only new data

Gold layer recalculates metrics safely

Run:

python incremental/incremental_load.py



---- Outputs ----
Silver Layer

Cleaned and validated campaign data

Business-rule filtered records

Gold Layer

Campaign Summary

Channel Summary

Daily Summary

SQL-based performance metrics



---- Data Validation Checks ----

Null checks on critical columns

Negative value detection

CTR & Conversion Rate range validation

Silver vs Gold totals consistency



---- Sample Metrics Generated ----

Click Through Rate (CTR)

Conversion Performance Category

ROI Risk Level

Campaign Performance Summary



---- Logging ----

Console logs for runtime monitoring

JSON logs for audit & debugging

Stored in /logs directory