import os
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# ==============================================================================
# CONFIGURATION ET SETUP
# ==============================================================================
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "data-proj-mohamed")
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

LOCAL_DATA_DIR = "/tmp/insurance_data"

default_args = {
    "owner": "data_engineers",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ==============================================================================
# FONCTIONS PYTHON (EXTRACTION & GENERATION S3)
# ==============================================================================
def generate_synthetic_data(**context):
    """Génère des datasets synthétiques d'assurance."""
    fake = Faker("fr_FR")
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    
    # 1. Assurés
    clients = []
    for _ in range(200):
        clients.append({
            "client_id": fake.uuid4()[:8],
            "nom": fake.last_name(),
            "prenom": fake.first_name(),
            "age": fake.random_int(min=18, max=75),
            "ville": fake.random_element(["Casablanca", "Rabat", "Tanger", "Marrakech", "Agadir"]),
            "profession": fake.job(),
            "score_risque": round(fake.random_number(digits=2) / 10.0, 2),
            "created_at": fake.date_between(start_date="-3y", end_date="today").strftime("%Y-%m-%d")
        })
    df_clients = pd.DataFrame(clients)
    df_clients.to_csv(f"{LOCAL_DATA_DIR}/assures.csv", index=False)

    # 2. Contrats
    contrats = []
    for i in range(300):
        client = df_clients.sample(1).iloc[0]
        contrats.append({
            "contrat_id": f"CTR-{1000 + i}",
            "client_id": client["client_id"],
            "type_couverture": fake.random_element(["Tiers", "Tiers Étendu", "Tous Risques"]),
            "type_vehicule": fake.random_element(["Berline", "SUV", "Citadine", "Utilitaire"]),
            "prime_annuelle_mad": fake.random_int(min=3000, max=15000),
            "date_souscription": fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d"),
            "statut_contrat": fake.random_element(["Actif", "Résilié", "Suspendu"])
        })
    df_contrats = pd.DataFrame(contrats)
    df_contrats.to_csv(f"{LOCAL_DATA_DIR}/contrats.csv", index=False)

    # 3. Sinistres
    sinistres = []
    for i in range(150):
        contrat = df_contrats.sample(1).iloc[0]
        reclame = fake.random_int(min=2000, max=50000)
        statut = fake.random_element(["Accepté", "En cours", "Refusé"])
        rembourse = reclame * fake.random_element([0.8, 1.0]) if statut == "Accepté" else 0.0
        
        sinistres.append({
            "sinistre_id": f"SIN-{5000 + i}",
            "contrat_id": contrat["contrat_id"],
            "date_sinistre": fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d"),
            "type_sinistre": fake.random_element(["Accident", "Vol", "Incendie", "Bris de glace"]),
            "montant_reclame_mad": reclame,
            "montant_rembourse_mad": round(rembourse, 2),
            "statut_sinistre": statut,
            "suspicion_fraude": fake.random_element([0, 0, 0, 0, 1])
        })
    df_sinistres = pd.DataFrame(sinistres)
    df_sinistres.to_csv(f"{LOCAL_DATA_DIR}/sinistres.csv", index=False)


def upload_to_s3(**context):
    """Envoie les fichiers CSV générés sur AWS S3."""
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    files = ["assures.csv", "contrats.csv", "sinistres.csv"]
    for file in files:
        local_path = f"{LOCAL_DATA_DIR}/{file}"
        s3_key = f"raw/{file}"
        s3_hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )

# ==============================================================================
# DEFINITION DU DAG AIRFLOW
# ==============================================================================
with DAG(
    "s3_snowflake_etl",
    default_args=default_args,
    description="Pipeline Medallion Architecture (Bronze -> Silver -> Gold Kimball) dans SCHEMA PUBLIC_",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1. Génération données locales
    task_generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=generate_synthetic_data,
    )

    # 2. Upload S3
    task_upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    # --------------------------------------------------------------------------
    # 3. COUCHE BRONZE : DONNEES BRUTES (SCHEMA PUBLIC_)
    # --------------------------------------------------------------------------
    task_load_bronze = SnowflakeOperator(
        task_id="load_bronze",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE DEV_INSURANCE_DB;
            USE SCHEMA PUBLIC_;

            COPY INTO DEV_INSURANCE_DB.PUBLIC_.BRONZE_ASSURES
            FROM @S3_INSURANCE_STAGE/raw/assures.csv
            FILE_FORMAT = (FORMAT_NAME = 'CSV_INSURANCE_FORMAT')
            ON_ERROR = 'CONTINUE';

            COPY INTO DEV_INSURANCE_DB.PUBLIC_.BRONZE_CONTRATS
            FROM @S3_INSURANCE_STAGE/raw/contrats.csv
            FILE_FORMAT = (FORMAT_NAME = 'CSV_INSURANCE_FORMAT')
            ON_ERROR = 'CONTINUE';

            COPY INTO DEV_INSURANCE_DB.PUBLIC_.BRONZE_SINISTRES
            FROM @S3_INSURANCE_STAGE/raw/sinistres.csv
            FILE_FORMAT = (FORMAT_NAME = 'CSV_INSURANCE_FORMAT')
            ON_ERROR = 'CONTINUE';
        """,
    )

    # --------------------------------------------------------------------------
    # 4. COUCHE SILVER : DONNEES NETTOYEES ET RETRAITEES (SCHEMA PUBLIC_)
    # --------------------------------------------------------------------------
    task_transform_silver = SnowflakeOperator(
        task_id="transform_silver",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE DEV_INSURANCE_DB;
            USE SCHEMA PUBLIC_;

            -- Silver Assurés : Formatage des textes et segmentation âge
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.SILVER_ASSURES AS
            SELECT 
                client_id, 
                UPPER(TRIM(nom)) AS nom, 
                INITCAP(TRIM(prenom)) AS prenom, 
                age,
                CASE 
                    WHEN age < 25 THEN '18-24' 
                    WHEN age BETWEEN 25 AND 40 THEN '25-40' 
                    WHEN age BETWEEN 41 AND 60 THEN '41-60' 
                    ELSE '60+' 
                END AS tranche_age,
                TRIM(ville) AS ville, 
                TRIM(profession) AS profession, 
                score_risque, 
                CAST(created_at AS DATE) AS date_inscription
            FROM DEV_INSURANCE_DB.PUBLIC_.BRONZE_ASSURES;

            -- Silver Contrats : Typage propre et validation
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.SILVER_CONTRATS AS
            SELECT 
                contrat_id, 
                client_id, 
                TRIM(type_couverture) AS type_couverture, 
                TRIM(type_vehicule) AS type_vehicule, 
                CAST(prime_annuelle_mad AS DECIMAL(10, 2)) AS prime_annuelle_mad, 
                CAST(date_souscription AS DATE) AS date_souscription, 
                TRIM(statut_contrat) AS statut_contrat
            FROM DEV_INSURANCE_DB.PUBLIC_.BRONZE_CONTRATS;

            -- Silver Sinistres : Calcul du reste à charge et flags métiers
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.SILVER_SINISTRES AS
            SELECT 
                sinistre_id,
                contrat_id,
                CAST(date_sinistre AS DATE) AS date_sinistre,
                TRIM(type_sinistre) AS type_sinistre,
                CAST(montant_reclame_mad AS DECIMAL(10, 2)) AS montant_reclame_mad,
                CAST(montant_rembourse_mad AS DECIMAL(10, 2)) AS montant_rembourse_mad,
                ROUND(montant_reclame_mad - montant_rembourse_mad, 2) AS montant_reste_a_charge_mad,
                TRIM(statut_sinistre) AS statut_sinistre,
                CAST(suspicion_fraude AS INT) AS suspicion_fraude,
                CASE WHEN statut_sinistre = 'Accepté' THEN 1 ELSE 0 END AS est_accepte,
                CASE WHEN statut_sinistre = 'Refusé' THEN 1 ELSE 0 END AS est_refuse
            FROM DEV_INSURANCE_DB.PUBLIC_.BRONZE_SINISTRES;
        """,
    )

    # --------------------------------------------------------------------------
    # 5. COUCHE GOLD : MODELISATION DIMENSIONNELLE (DIM, FACT & VIEWS KIMBALL)
    # --------------------------------------------------------------------------
    task_load_gold = SnowflakeOperator(
        task_id="load_gold",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE DEV_INSURANCE_DB;
            USE SCHEMA PUBLIC_;

            -- A. DIMENSION TEMPS
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.DIM_TEMPS AS
            WITH date_range AS (
                SELECT DATEADD(day, SEQ4(), '2023-01-01'::DATE) AS date_day
                FROM TABLE(GENERATOR(ROWCOUNT => 1825))
            )
            SELECT
                date_day,
                YEAR(date_day) AS annee,
                QUARTER(date_day) AS trimestre,
                MONTH(date_day) AS mois,
                MONTHNAME(date_day) AS nom_mois,
                DAY(date_day) AS jour_mois,
                DAYOFWEEK(date_day) AS jour_semaine,
                WEEKOFYEAR(date_day) AS semaine_annee,
                CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS est_weekend
            FROM date_range;

            -- B. DIMENSION CLIENTS
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.DIM_CLIENTS AS
            SELECT 
                client_id, 
                nom, 
                prenom, 
                age,
                tranche_age,
                ville, 
                profession, 
                score_risque, 
                date_inscription
            FROM DEV_INSURANCE_DB.PUBLIC_.SILVER_ASSURES;

            -- C. DIMENSION CONTRATS
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.DIM_CONTRATS AS
            SELECT 
                contrat_id, 
                client_id, 
                type_couverture, 
                type_vehicule, 
                prime_annuelle_mad, 
                date_souscription, 
                statut_contrat
            FROM DEV_INSURANCE_DB.PUBLIC_.SILVER_CONTRATS;

            -- D. DIMENSION TYPE DE SINISTRE
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.DIM_TYPE_SINISTRE AS
            SELECT DISTINCT 
                DENSE_RANK() OVER (ORDER BY type_sinistre) AS type_sinistre_id,
                type_sinistre AS libelle_sinistre,
                CASE 
                    WHEN type_sinistre IN ('Accident', 'Incendie') THEN 'Élevée' 
                    WHEN type_sinistre IN ('Vol') THEN 'Moyenne' 
                    ELSE 'Faible' 
                END AS categorie_gravite
            FROM DEV_INSURANCE_DB.PUBLIC_.SILVER_SINISTRES;

            -- E. TABLE DE FAITS SINISTRES (Stricte Kimball : FKs + Métriques Numériques Additives)
            CREATE OR REPLACE TABLE DEV_INSURANCE_DB.PUBLIC_.FACT_SINISTRES AS
            SELECT 
                s.sinistre_id,
                s.date_sinistre,
                c.client_id,
                s.contrat_id,
                t.type_sinistre_id,
                s.montant_reclame_mad,
                s.montant_rembourse_mad,
                s.montant_reste_a_charge_mad,
                s.suspicion_fraude,
                s.est_accepte,
                s.est_refuse
            FROM DEV_INSURANCE_DB.PUBLIC_.SILVER_SINISTRES s
            JOIN DEV_INSURANCE_DB.PUBLIC_.SILVER_CONTRATS c ON s.contrat_id = c.contrat_id
            JOIN DEV_INSURANCE_DB.PUBLIC_.DIM_TYPE_SINISTRE t ON s.type_sinistre = t.libelle_sinistre;

        """,
    )

    # ==========================================================================
    # FLUX D'EXECUTION DU PIPELINE (DEPENDANCES)
    # ==========================================================================
    task_generate_data >> task_upload_s3 >> task_load_bronze >> task_transform_silver >> task_load_gold