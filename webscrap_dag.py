from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import python_operator
from airflow.utils.dates import days_age
from datetime import datetime

from webscrap import run_webscrap_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email': ['cklau2421@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'webscrap_dag',
    default_args=default_args,
    description='Webscrap DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_webscrap_etl',
    python_callable=run_webscrap_etl,
    dag=dag, 
)

run_etl