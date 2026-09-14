# End-to-End Insurance Data Warehouse: AWS S3, Snowflake, Airflow & Tableau

An enterprise-grade, end-to-end Data Engineering pipeline orchestrating insurance data ingestion from **AWS S3** into **Snowflake**, transforming raw datasets using a **Medallion Architecture** with **dbt**, automated via **Apache Airflow**, and visualized through an interactive **Tableau** dashboard.

## Tech Stack
* **Orchestration**: Apache Airflow
* **Cloud Storage**: AWS S3
* **Data Warehouse**: Snowflake
* **Transformation & Testing**: dbt (Data Build Tool)
* **Business Intelligence**: Tableau Desktop
* **Containerization**: Docker & Docker Compose

## Architecture
![Architecture du Pipeline](docs/image_pipeline_etl_s3_snowflake.jpg)

## Pipeline Components

### 1. Ingestion Layer (`AWS S3 -> Snowflake Bronze`)
* **Source Datasets**: Core Insurance Data (Assurés, Contrats, Sinistres).
* **Ingestion Logic**: Airflow DAG orchestrates raw CSV file staging on **AWS S3** and loads data into Snowflake using optimized `COPY INTO` commands.
* **Bronze Layer**: Raw staging zone with schema-on-read capabilities and audit metadata (`ingested_at`).

### 2. Transformation Layer (`dbt & Medallion Pattern`)
* **Silver Layer (`SILVER`)**: Data cleaning, strict typing, deduplication, and standardization across entities using **dbt** models.
* **Gold Layer (`GOLD`)**: Analytics-ready Data Marts aggregating key business metrics (sinistralité, indemnisation, ratios S/P).
* **Data Quality**: Enforced via `dbt test` (uniqueness, non-nullability, foreign key integrity).

### 3. Analytics & Visualization (`Tableau`)
* **Data Source**: Live connection to Snowflake Gold layer.
* **Key Visuals**: KPI Scorecards (Total Sinistres, Ratio S/P), monthly trend analysis, risk breakdown by insurance policy type, and demographic analysis.

### 4. Orchestration (`Apache Airflow`)
* **Pipeline Flow**: `upload_s3` ➔ `load_bronze` ➔ `run_dbt_silver` ➔ `run_dbt_gold`
* **Reliability**: Configured task retries, error handling, and scheduled executions.