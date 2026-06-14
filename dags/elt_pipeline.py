# ./dags/elt_pipeline.py
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Permite importar el script de ingesta
sys.path.append('/opt/airflow')
from scripts.ingestion import run_ingestion

default_args = {
    'owner': 'uteq_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='elt_ecommerce_pipeline',
    default_args=default_args,
    description='Pipeline automatizado de Ingesta para E-commerce UTEQ',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 27),
    catchup=False,
) as dag:

    task_extract_load = PythonOperator(
        task_id='Extract_and_Load',
        python_callable=run_ingestion,
    )
