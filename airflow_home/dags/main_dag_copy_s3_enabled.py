from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os

# Import extraction functions
from mock_extract_fema_dag import extract_fema_data  # FEMA Extraction
from mock_extract_bls_dag import run_bls_extraction
from mock_extract_climate_dag import extract_climate_data

# Import transformation functions
from mock_fema_transform_dag import transform_fema_data  # FEMA Transformation
from mock_bls_transform_dag import transform_bls_data
from mock_climate_transform_dag import transform_climate_data

# AWS S3 Configuration
BUCKET_NAME = "507etlbucketbeforedb"

default_args = {
    "start_date": datetime(2023, 1, 1),
}

def upload_to_s3(**kwargs):
    """Uploads transformed FEMA, BLS, and Climate data to S3."""
    s3 = boto3.client('s3')

    # Fetch file paths from XCom
    ti = kwargs['ti']
    fema_file_path = ti.xcom_pull(task_ids="transform_fema_data")
    bls_file_path = ti.xcom_pull(task_ids="transform_bls_data")
    climate_file_path = ti.xcom_pull(task_ids="transform_climate_data")

    # Define S3 keys
    s3_files = {
        "data/transformed_fema.csv": fema_file_path,
        "data/transformed_bls.csv": bls_file_path,
        "data/merged_climate_data.csv": climate_file_path,
    }

    # Upload files to S3
    for s3_key, file_path in s3_files.items():
        if file_path and os.path.exists(file_path):
            s3.upload_file(file_path, BUCKET_NAME, s3_key)
            print(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{s3_key}")
        else:
            print(f"File {file_path} not found. Skipping upload.")

with DAG(
    dag_id="main_dag_copy_s3_enabled",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="A DAG that extracts, transforms, and uploads FEMA, BLS, and Climate data to S3",
) as dag:

    # Extraction tasks
    extract_fema_task = PythonOperator(
        task_id="extract_fema_data",
        python_callable=extract_fema_data,
        provide_context=True,  # Enables XCom passing
    )

    extract_bls_task = PythonOperator(
        task_id="extract_bls_data",
        python_callable=run_bls_extraction,
        provide_context=True,
    )

    extract_climate_task = PythonOperator(
        task_id="extract_climate_data",
        python_callable=extract_climate_data,
        provide_context=True,
    )

    # Transformation tasks
    transform_fema_task = PythonOperator(
        task_id="transform_fema_data",
        python_callable=transform_fema_data,
        provide_context=True,  # Ensures FEMA data is passed dynamically
    )

    transform_bls_task = PythonOperator(
        task_id="transform_bls_data",
        python_callable=transform_bls_data,
        provide_context=True,
    )

    transform_climate_task = PythonOperator(
        task_id="transform_climate_data",
        python_callable=transform_climate_data,
        provide_context=True,
    )

    # S3 Upload Task
    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    # Define dependencies
    [extract_fema_task, extract_bls_task, extract_climate_task] >> [transform_fema_task, transform_bls_task, transform_climate_task] >> upload_s3_task
