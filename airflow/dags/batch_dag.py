from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_analytics',
    default_args=default_args,
    description='Run batch analytics on parking data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

run_batch = BashOperator(
    task_id='run_batch_analytics',
    bash_command='cd /opt/airflow/jobs && python batch_analytics.py',
    dag=dag,
)