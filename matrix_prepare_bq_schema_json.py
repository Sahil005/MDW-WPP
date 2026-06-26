from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
import json
from google.cloud import storage, bigquery
import matrix_config

# Configs from matrix_config
bq_config = matrix_config.BigQueryConfig()
DATA_BUCKET = bq_config.dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='matrix_prepare_booking_spot_table',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Create the unified BigQuery table "booking_spot" using schema JSON from GCS.',
)
def create_booking_spot_table():

    @task
    def create_table():
        bucket_name = "au-accel-mdw-dev-data"
        folder_path = "table_schemas/L01_raw_matrix/matrix_tables_booking_spot_mb"
        schema_file = "matrix_tables_booking_spot_mb_schema.json"

        storage_client = storage.Client()
        bq_client = bigquery.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{folder_path}/{schema_file}")

        if not blob.exists():
            raise FileNotFoundError(f"Schema file not found: gs://{bucket_name}/{folder_path}/{schema_file}")

        schema_content = blob.download_as_string()
        schema = json.loads(schema_content)

        table_name = "booking_spot"  
        table_id = f"{bq_client.project}.{DATA_BUCKET}.{table_name}"

        table = bigquery.Table(
            table_id,
            schema=[
                bigquery.SchemaField(field['name'], field['type'], field.get('mode', 'NULLABLE'))
                for field in schema
            ]
        )

        try:
            bq_client.create_table(table)
            print(f"Created BigQuery table: {table_id}")
        except Exception as e:
            print(f"Failed to create table {table_id}: {e}")

    create_table()

create_booking_spot_table()
