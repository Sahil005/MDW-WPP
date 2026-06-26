import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator
)
from airflow.models import DagRun
from airflow.utils.state import DagRunState
import matrix_config_v3

# Load v3 configuration (isolated from production)
bq = matrix_config_v3.BigQueryConfig()
matrix = matrix_config_v3.MatrixConfig()
compute = matrix_config_v3.ComputeConfig()
storage = matrix_config_v3.StorageConfig()

PROJECT_ID = bq.project_id
# Use test bucket from v3 configuration
TEST_BUCKET = storage.data_bucket
GCE_INSTANCE = compute.instance_name
GCE_ZONE = compute.zone
USER = compute.user
SERVER_NAME = matrix.server_name
CATALOG = matrix.catalog
USER_ID_SECRET = matrix.user_id_secret
PASSWORD_SECRET = matrix.password_secret

# Copy v3 scripts from GCS
setup_command = (
    # "gsutil -m cp -r \"gs://au-accel-mdw-dev-scripts/matrix_export_v3/*\" \"./scripts_v3/\""
    "gsutil -m cp -r \"gs://au-accel-mdw-dev-scripts/matrix_export_v3/*\" C:/Users/v_manteshwar_hajare/scripts_v3/"
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_extraction_period_with_gcs_path(execution_date: datetime, client_name: str):
    """
    Simple monthly extraction - extract the previous month's data
    
    For a monthly schedule running on first Monday of each month:
    - When running in October 2025 (logical_date ~ 2025-10-06)
    - Extract September 2025 data (2025-09-01 to 2025-09-30)
    
    Args:
        execution_date: Should be the data_interval_end (logical date) for monthly schedules
        client_name: Client name for GCS path generation
    
    Returns:
        dict: Contains start_date, end_date, and gcs_path for previous month
    """
    logging.info(f"Processing monthly extraction for logical date: {execution_date}")
    
    # Get the previous month
    if execution_date.month == 1:
        previous_month = execution_date.replace(year=execution_date.year - 1, month=12, day=1)
    else:
        previous_month = execution_date.replace(month=execution_date.month - 1, day=1)
    
    # Calculate end of previous month
    if previous_month.month == 12:
        end_of_month = previous_month.replace(year=previous_month.year + 1, month=1) - timedelta(days=1)
    else:
        end_of_month = previous_month.replace(month=previous_month.month + 1) - timedelta(days=1)
    
    start_date = previous_month.strftime('%Y-%m-%d')
    end_date = end_of_month.strftime('%Y-%m-%d')
    
    # Create GCS path with monthly partitioning
    gcs_path = f"matrix/raw/partition_year={previous_month.year}/partition_month={previous_month.month:02d}/partition_client={client_name.lower()}"
    
    logging.info(f"Calculated extraction period: {start_date} to {end_date}")
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'gcs_path': gcs_path
    }

def create_new_dynamic_dag(dag_id: str, client_name: str, client_ids: str, start_date: str):
    @dag(
        dag_id=f"{dag_id}_v3",  # New versioned DAG ID
        default_args={**default_args, 'start_date': datetime.strptime(start_date, '%Y-%m-%d')},
        description=f'Monthly extraction for {client_name} with BigLake integration.',
        schedule='0 8 * * 1#1',  # First Monday of each month at 8am
        catchup=True,  # Enable catchup to process historical months
        max_active_runs=1,
        tags=['matrix', 'v3', 'monthly', 'biglake'],
        doc_md=f"""
        # Monthly Matrix Extraction DAG v3 - {client_name}
        
        This DAG extracts one month of data per run:
        
        **Monthly Schedule:**
        - Runs on the first Monday of each month at 8am
        - Extracts the previous month's data (avoids weekend database downtime)
        - With catchup=True, will backfill from start_date to current month
        - Writes to: `matrix/raw/partition_year=YYYY/partition_month=MM/partition_client={client_name.lower()}/booking_spot.csv`
        
        **BigLake Integration:**
        - Data immediately available in BigQuery via external table
        - Hive-partitioned structure optimizes query performance
        - No separate load DAG required
        
        **Simple & Reliable:**
        - One month per run - proven reliable with Matrix legacy systems
        - Standard monthly schedule aligns with business needs
        - Catchup handles historical data automatically
        
        **Client Configuration:**
        - Client IDs: {client_ids}
        - Historical Start: {start_date}
        - Target Bucket: {TEST_BUCKET}
        """
    )
    def taskflow():
        start_instance = ComputeEngineStartInstanceOperator(
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

        @task
        def determine_extraction_parameters(**context):
            """Determine extraction month based on data interval end (logical date)"""
            # Use data_interval_end which represents the logical date for monthly schedules
            logical_date = context['data_interval_end']
            params = get_extraction_period_with_gcs_path(
                execution_date=logical_date,
                client_name=client_name
            )
            
            logging.info(f"Extraction parameters for {client_name}: {params}")
            logging.info(f"Using logical_date (data_interval_end): {logical_date}")
            logging.info(f"Execution_date was: {context['execution_date']}")
            return params

        @task_group(group_id='matrix_extract_to_gcs')
        def extract():
            
            def run_extraction(**context):
                task_params = context['task_instance'].xcom_pull(task_ids='determine_extraction_parameters')
                
                final_command = f"""cmd.exe /c python scripts_v3\\main.py 
                    --project-id={PROJECT_ID} 
                    --destination-bucket={TEST_BUCKET} 
                    --output-path={task_params['gcs_path']}
                    --server-name={SERVER_NAME} 
                    --catalog={CATALOG} 
                    --user-id-secret={USER_ID_SECRET} 
                    --password-secret={PASSWORD_SECRET} 
                    --start-date={task_params['start_date']}
                    --end-date={task_params['end_date']}
                    --client-ids={client_ids}
                    --client-name="{client_name}" 
                    --source-table="booking_spot_2"
                    --output-filename="booking_spot.csv"
                """.replace('\n', ' ').replace('  ', ' ').strip()
                
                logging.info(f"Running monthly extraction for {client_name}")
                logging.info(f"Command: {final_command}")
                
                return SSHOperator(
                    task_id=f'extract_{client_name}_table',
                    ssh_conn_id='ssh_default',
                    command=final_command,
                    cmd_timeout=30000,  # 30 seconds for monthly data
                    doc_md=f'Monthly extraction for {client_name} - {task_params["start_date"]} to {task_params["end_date"]}'
                ).execute(context)
            
            run_extraction_task = task(run_extraction)
            return run_extraction_task()

        stop_instance = ComputeEngineStopInstanceOperator(
            task_id="stop_gce_instance",
            project_id=PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE
        )

        start_instance >> copy_scripts >> determine_extraction_parameters() >> extract() >> stop_instance
        # start_instance >> determine_extraction_parameters() >> extract() >> stop_instance

    return taskflow()


config_v3 = matrix_config_v3.Config()

for client_config in config_v3.export_list:
    dag_id = f'matrix_export_{client_config["client_name"]}_to_gcs'
    client_name = client_config['client_name']
    client_ids = client_config['client_ids']
    start_date = client_config['start_date']

    # Create the new dynamic DAG (inside the loop!)
    globals()[f"{dag_id}_v3"] = create_new_dynamic_dag(dag_id, client_name, client_ids, start_date)
