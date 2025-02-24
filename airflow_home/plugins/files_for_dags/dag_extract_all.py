# multi_extraction_etl_dag.py

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import extraction functions.
# STAY IN same directory as DAG.
from mock_extract_fema_dag import extract_fema_data  # FEMA API extraction function
from mock_extract_bls_dag import run_bls_extraction   # BLS API extraction function

default_args = {
    "start_date": datetime(2023, 1, 1),
    # Add other default args if needed (e.g., retries, email alerts)
}

with DAG(
    dag_id="multi_extraction_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",  # schedules the pull daily
    catchup=False,
    description="A DAG that extracts data from FEMA and BLS APIs",
) as dag:

    extract_fema_task = PythonOperator(
        task_id="extract_fema_data",
        python_callable=extract_fema_data,
    )

    extract_bls_task = PythonOperator(
        task_id="extract_bls_data",
        python_callable=run_bls_extraction,
    )

    # run both extraction tasks in parallel:
    [extract_fema_task, extract_bls_task]

   #Or if we want to chain them like this:
    # extract_fema_task >> extract_bls_task

