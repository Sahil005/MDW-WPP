from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryDeleteTableOperator
from datetime import timedelta
from google.cloud import storage
import json
from datetime import datetime, timedelta
import matrix_config

# Global Variables
CLIENTS = ['cba', 'dpe']

MAIN_TABLE = "booking_spot"
STAGING_TABLE = "booking_spot_staging"
SCHEMA_PATH = "table_schemas/L01_raw_matrix/matrix_tables_booking_spot_mb/matrix_tables_booking_spot_mb_schema.json"

# Load configuration
bq = matrix_config.BigQueryConfig()
matrix = matrix_config.MatrixConfig()
compute = matrix_config.ComputeConfig()
bucket = matrix_config.StorageConfig()

PROJECT_ID = bq.project_id
DATASET = bq.dataset
GCS_BUCKET = bucket.data_bucket
execution_date = datetime.today()
CALENDAR_WEEK  = (execution_date + timedelta(days=(6 - execution_date.weekday()))).strftime('%Y%m%d')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_schema_from_gcs():
    """Load schema from GCS JSON file."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(SCHEMA_PATH)

    if not blob.exists():
        raise FileNotFoundError(f"Schema not found at: gs://{GCS_BUCKET}/{SCHEMA_PATH}")

    schema_json = json.loads(blob.download_as_string())
    return [
        {"name": field["name"], "type": field["type"], "mode": field.get("mode", "NULLABLE")}
        for field in schema_json
    ]

with DAG(
    dag_id='load_booking_spot_to_bq',
    default_args=default_args,
    description='Load unified booking spot data into a single BigQuery table with schema and deduplication',
    schedule_interval='0 3 * * 0',  # Weekly at 3 AM Sunday
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    schema_fields = load_schema_from_gcs()

    # Load all CSV chunks from GCS for each client
    load_tasks = []
    for client in CLIENTS:
        load = GCSToBigQueryOperator(
            task_id=f'load_{client}_chunks_to_staging',
            bucket=GCS_BUCKET,
            source_objects=[f"matrix/tables/booking_spot_{client}/{CALENDAR_WEEK}.csv"],  
            destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{STAGING_TABLE}',
            skip_leading_rows=1,
            source_format='CSV',
            write_disposition='WRITE_APPEND',  # Append all chunks into staging
            create_disposition='CREATE_IF_NEEDED',
            schema_fields=schema_fields,
            autodetect=False
        )
        load_tasks.append(load)

    # Merge staging into main table
    merge_to_main = BigQueryInsertJobOperator(
        task_id='merge_staging_to_main',
        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{DATASET}.{MAIN_TABLE}` T
                    USING (
                        SELECT * FROM `{PROJECT_ID}.{DATASET}.{STAGING_TABLE}`
                    ) S
                    ON T.SpotId = S.SpotId
                    WHEN NOT MATCHED THEN
                        INSERT ROW
                """,
                "useLegacySql": False
            }
        }
    )

    # Drop staging table after merge
    drop_staging = BigQueryDeleteTableOperator(
        task_id='drop_staging',
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}",
        ignore_if_missing=True
    )

    load_tasks >> merge_to_main >> drop_staging
