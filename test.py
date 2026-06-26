from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator 
from datetime import datetime
 
with DAG('test_gcp_ssh', start_date=datetime(2024, 1, 1), schedule_interval=None) as dag:
 
    ssh_task = SSHOperator(
        task_id='run_command_on_gcp_vm',
        ssh_conn_id='ssh_default',
        command='hostname',
        do_xcom_push=True,
    )

  
