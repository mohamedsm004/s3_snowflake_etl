import io
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

BUCKET_NAME = "data-proj-mohamed"
API_URL = "https://api.sampleapis.com/cartoons/cartoons2D"

default_args = {
    "owner": "mohamed",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def extract_api_and_upload_to_s3():
    """Extrait les données de l'API et les dépose sur S3 via S3Hook."""
    print("1. Extraction des données depuis l'API...")
    response = requests.get(API_URL)

    if response.status_code != 200:
        raise Exception(f"Erreur API. Code statut: {response.status_code}")

    data = response.json()
    df = pd.DataFrame(data)

    df_clean = df[["id", "title", "year"]].copy()

    csv_buffer = io.StringIO()
    df_clean.to_csv(csv_buffer, index=False)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/cartoons_{timestamp}.csv"

    print(f"2. Téléversement vers S3 : {s3_key}...")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    print("   -> Succès S3 !")

with DAG(
    dag_id="s3_to_snowflake_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet : API -> S3 -> Bronze -> Silver -> Gold",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data_engineering", "snowflake", "medallion"],
) as dag:

    # 1. Extraction et chargement vers AWS S3
    task_extract_to_s3 = PythonOperator(
        task_id="extract_api_to_s3",
        python_callable=extract_api_and_upload_to_s3,
    )

    # 2. Ingestion S3 vers couche BRONZE (Raw)
    task_load_to_bronze = SnowflakeOperator(
        task_id="load_s3_to_bronze",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO STAGE_DB.BRONZE.CARTOONS_RAW (id, title, date_year, ingested_at)
            FROM (
                SELECT $1, $2, $3, CURRENT_TIMESTAMP()
                FROM @STAGE_DB.RAW_DATA.S3_STAGE
            )
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            PATTERN = '.*cartoons_.*\\.csv'
            ON_ERROR = 'CONTINUE';
        """,
    )

    # 3. Transformation BRONZE -> SILVER (Dédoublonnage & Typage)
    task_transform_to_silver = SnowflakeOperator(
        task_id="transform_bronze_to_silver",
        snowflake_conn_id="snowflake_default",
        sql="""
            MERGE INTO SILVER.DIM_CARTOONS AS target
            USING (
                WITH ranked_data AS (
                    SELECT 
                        TRY_CAST(TRIM(id) AS INT) AS clean_id,
                        TRIM(title) AS clean_title,
                        TRY_CAST(TRIM(date_year) AS INT) AS clean_year,
                        ingested_at,
                        -- Dédoublonnage : Conserve l'enregistrement le plus récent par ID
                        ROW_NUMBER() OVER (
                            PARTITION BY TRY_CAST(TRIM(id) AS INT) 
                            ORDER BY ingested_at DESC
                        ) AS row_num
                    FROM BRONZE.CARTOONS_RAW
                    WHERE id IS NOT NULL 
                    AND TRIM(id) != ''
                    AND TRY_CAST(TRIM(id) AS INT) IS NOT NULL
                )
                SELECT 
                    clean_id,
                    clean_title,
                    clean_year,
                    ingested_at
                FROM ranked_data
                WHERE row_num = 1
            ) AS source
            ON target.cartoon_id = source.clean_id

            -- Mise à jour si le titre ou l'année a changé
            WHEN MATCHED AND (target.title != source.clean_title OR target.release_year != source.clean_year) THEN
                UPDATE SET 
                    target.title = source.clean_title,
                    target.release_year = source.clean_year,
                    target.updated_at = CURRENT_TIMESTAMP()

            -- Insertion si l'ID n'existe pas encore dans la couche Silver
            WHEN NOT MATCHED THEN
                INSERT (cartoon_id, title, release_year, created_at)
                VALUES (source.clean_id, source.clean_title, source.clean_year, source.ingested_at);
        """,
    )

    # 4. Rafraîchissement de la vue GOLD (Analytics)
    task_refresh_gold = SnowflakeOperator(
        task_id="refresh_gold_layer",
        snowflake_conn_id="snowflake_default",
        sql="""
            CREATE OR REPLACE VIEW STAGE_DB.GOLD.VW_CARTOONS_SUMMARY AS
            SELECT 
                release_year,
                COUNT(cartoon_id) AS total_cartoons,
                MIN(created_at) AS first_ingested_at,
                MAX(updated_at) AS last_updated_at
            FROM STAGE_DB.SILVER.DIM_CARTOONS
            WHERE release_year IS NOT NULL
            GROUP BY release_year;
                    """,
    )

    # Définition des dépendances
    task_extract_to_s3 >> task_load_to_bronze >> task_transform_to_silver >> task_refresh_gold