#  Cloud Data Pipeline: API to Snowflake via AWS S3 & Airflow

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-Bucket-FF9900?logo=amazonaws)](https://aws.amazon.com/s3/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Cloud-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)

An end-to-end, enterprise-grade Data Engineering pipeline orchestrating data extraction from REST APIs, staging raw files on **AWS S3**, ingesting and transforming data through a **Medallion Architecture** in **Snowflake**, fully automated using **Apache Airflow** and secured with **RSA Key-Pair Authentication**.

---
##  Key Features & Technical Highlights

* **Automated Data Pipeline**: Daily orchestrated ETL workflow extracting JSON payload from APIs, formatting into structured CSVs, and pushing to AWS S3.
* **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**:
  * 🥉 **Bronze (`BRONZE_RAW`)**: Raw landing zone loading append-only data using Snowflake `COPY INTO` with auto-timestamping.
  * 🥈 **Silver (`SILVER_CLEANED`)**: Cleaned, typed, and deduplicated dimension tables managed incrementally via SQL `MERGE INTO` statement with `ROW_NUMBER()` window functions.
  * 🥇 **Gold (`GOLD_ANALYTICS`)**: Business-ready analytical views and data marts for BI dashboards (e.g., Power BI / Tableau).
* **Enterprise Security & Compliance**:
  * **Key-Pair Authentication (RSA 2048-bit)**: Passwordless connection between Airflow and Snowflake using encrypted private key.
* **Containerized Infrastructure**: Fully reproducible local execution environment powered by `Docker Compose` hosting Airflow Scheduler, Webserver, and Postgres metadata store.

---

##  Tech Stack & Prerequisites

* **Languages**: Python 3.10+, SQL (Snowflake Dialect)
* **Orchestration**: Apache Airflow 2.8+
* **Cloud Storage**: AWS S3
* **Data Warehouse**: Snowflake
* **Containerization**: Docker & Docker Compose
* **Security & Auth**: OpenSSL / Cryptography (RSA Key Pairs)

---

##  Pipeline DAG Workflow (Airflow Tasks)

The Airflow DAG (`s3_to_snowflake_pipeline`) consists of 4 sequential tasks:

1. `extract_api_to_s3`: Extracts API endpoints, transforms JSON to CSV in memory using `pandas`, and streams to S3 bucket using `S3Hook`.
2. `load_s3_to_bronze`: Executes `COPY INTO` on Snowflake to ingest S3 data into `BRONZE_RAW.CARTOONS_RAW`.
3. `transform_bronze_to_silver`: Runs deduplication, type casting (`TRY_CAST`), and upserts into `SILVER_CLEANED.DIM_CARTOONS` using `MERGE INTO`.
4. `refresh_gold_layer`: Re-creates or refreshes analytical summary views in `GOLD_ANALYTICS.VW_CARTOONS_SUMMARY`.