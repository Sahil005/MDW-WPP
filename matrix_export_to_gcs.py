import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task_group
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator
)
import matrix_config

# Load configuration
bq = matrix_config.BigQueryConfig()
matrix = matrix_config.MatrixConfig()
compute = matrix_config.ComputeConfig()
storage = matrix_config.StorageConfig()

PROJECT_ID = bq.project_id
DATA_BUCKET = storage.data_bucket
GCE_INSTANCE = compute.instance_name
GCE_ZONE = compute.zone
USER = compute.user
SERVER_NAME = matrix.server_name
CATALOG = matrix.catalog
USER_ID_SECRET = matrix.user_id_secret
PASSWORD_SECRET = matrix.password_secret

# Calculate start and end of current week
execution_date = datetime.today()
start_date_this_week = (execution_date - timedelta(days=execution_date.weekday())).strftime('%Y-%m-%d')
end_date_this_week = (execution_date + timedelta(days=(6 - execution_date.weekday()))).strftime('%Y-%m-%d')
CALENDAR_WEEK  = (execution_date + timedelta(days=(6 - execution_date.weekday()))).strftime('%Y%m%d')
# Copy scripts from GCS
setup_command = (
    "gsutil -m cp -r \"gs://au-accel-mdw-dev-scripts/matrix_export/*\" .scripts\\"
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dag(dag_id, client_name, client_id, start_date):
    @dag(
        dag_id=dag_id,
        default_args={**default_args, 'start_date': start_date},
        description=f'Extract {client_name} data from Matrix to GCS.',
        schedule='@weekly',
        catchup=False,
        max_active_runs=1,
    )
    def taskflow():
        start = ComputeEngineStartInstanceOperator(
            task_id="start_gce_instance_export",
            project_id=PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE
        )

        copy_scripts = SSHOperator(
            task_id='load_dependencies',
            ssh_conn_id='ssh_default',
            command=setup_command,
            cmd_timeout=600
        )

        @task_group(group_id='matrix_extract_to_gcs')
        def extract():
            extract_command = f"""cmd.exe /c python .scripts/main.py 
                --project-id={PROJECT_ID} 
                --destination-bucket={DATA_BUCKET} 
                --server-name={SERVER_NAME} 
                --catalog={CATALOG} 
                --user-id-secret={USER_ID_SECRET} 
                --password-secret={PASSWORD_SECRET} 
                --calendar-week={CALENDAR_WEEK} 
                --start-date={start_date_this_week} 
                --end-date={end_date_this_week} 
                --client-ids={client_id}
                --client-name="{client_name}" 
                --source-table="booking_spot_2"
            """
        

            logging.info(f"Running extract command for {client_name}: {extract_command}")

            SSHOperator(
                task_id=f'extract_{client_name}_table',
                ssh_conn_id='ssh_default',
                command=extract_command.replace('\n', ' '),
                cmd_timeout=11000,
                doc_md=f'Export for {client_name}'
            )

        stop = ComputeEngineStopInstanceOperator(
            task_id="stop_gce_instance",
            project_id=PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE
        )

        start >> copy_scripts >> extract() #>> stop

    return taskflow()

# Dynamically create DAGs from export_list
for export in matrix_config.Config().export_list:
    if export['client_name'] in ['cba', 'dpe']: 
        dag_id = f'matrix_export_{export["client_name"]}_to_gcs'
        client_name = export['client_name']
        client_ids_N = export['client_ids']
        start_date = datetime.strptime(export['start_date'], '%Y-%m-%d').strftime('%Y-%m-%d')
        globals()[dag_id] = create_dag(dag_id, client_name, client_ids_N, start_date)
