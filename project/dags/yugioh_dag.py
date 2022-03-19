from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from yugioh_etl import run_yugioh_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'yugioh_dag',
    default_args=default_args,
    description='YuGiOh DAG',
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id='whole_yugioh_etl',
    python_callable=run_yugioh_etl,
    dag=dag
)

run_etl