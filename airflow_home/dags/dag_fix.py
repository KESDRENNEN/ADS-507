from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

#import extraction functions
#from files_for_dags.mock_extract_fema_dag import extract_fema_data  # FEMA Extraction
#from files_for_dags.mock_extract_fema_dag import extract_fema_data
from files_for_dags.extract_bls import main_bls_pull

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="dag_fix",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="A DAG that extracts, transforms, and uploads FEMA, BLS, and Climate data to S3",
) as dag:
    # Extraction tasks
    extract_bls_task = PythonOperator(
        task_id="extract_bls_data",
        python_callable=main_bls_pull,
    )