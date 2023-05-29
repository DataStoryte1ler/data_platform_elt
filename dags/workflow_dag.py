from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from api_extraction_stage import api_elt
from files_extraction_stage import elt_files
from dwh_tables_creation import create_tables
from transformation_stage import process_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

with DAG(
    'workflow_dag',
    default_args=default_args,
    description='Our data platform whole workflow',
    schedule_interval='@daily'
) as dag:

    run_api_extraction = PythonOperator(
        task_id='api_data_extraction',
        python_callable=api_elt
    )

    run_files_extraction = PythonOperator(
        task_id='files_data_extraction',
        python_callable=elt_files
    )

    run_dwh_tables_creation = PythonOperator(
        task_id='dwh_tables_creation',
        python_callable=create_tables
    )

    run_data_transformation = PythonOperator(
        task_id='data_transformation',
        python_callable=process_data
    )
    
[run_api_extraction, run_files_extraction] >> run_dwh_tables_creation >> run_data_transformation


