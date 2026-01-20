from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


VENV_PYTHON = "/home/sergi/airflow/airflow_venv/bin/python"
SPARK_SCRIPT = "/mnt/c/Users/sergi/OneDrive/Desktop/python_spark/football_etl.py"

default_args = {
    'owner': 'sergi',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'football_medallion_pipeline',
    default_args=default_args,
    description='Run Spark ETL from Airflow every minute',
    schedule_interval='* * * * *',  # Every 1 minute
    catchup=False,
    max_active_runs=1,     
    tags=['spark', 'football']
) as dag:

    run_spark = BashOperator(
        task_id='run_spark_etl',
        bash_command=f"{VENV_PYTHON} {SPARK_SCRIPT}"
    )
