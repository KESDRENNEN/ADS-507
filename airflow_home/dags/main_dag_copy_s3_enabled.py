from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3

# Import extraction functions.
from mock_extract_fema_dag import extract_fema_data   # FEMA API extraction function
from mock_extract_bls_dag import run_bls_extraction   # BLS API extraction function

# Import transformation functions.
from Mock_fema_cleaned_dag import transform_fema_data   # FEMA data transform function
from Mock_bls_cleaned_dag import transform_bls_data     # BLS data transform function

# AWS S3 Configuration
BUCKET_NAME = "507etlbucketbeforedb"

def upload_to_s3():
    """Uploads transformed FEMA and BLS data to S3."""
    s3 = boto3.client('s3')

    # Define file paths (assuming transformed data is saved as CSV files)
    fema_file_path = "/path/to/transformed_fema.csv"
    bls_file_path = "/path/to/transformed_bls.csv"

    # Upload files to S3
    s3.upload_file(fema_file_path, BUCKET_NAME, "data/transformed_fema.csv")
    s3.upload_file(bls_file_path, BUCKET_NAME, "data/transformed_bls.csv")

    print(f"Files uploaded to S3: {BUCKET_NAME}/data/transformed_fema.csv & {BUCKET_NAME}/data/transformed_bls.csv")

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="multi_extraction_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Run the DAG daily
    catchup=False,
    description="A DAG that extracts data from FEMA and BLS APIs, transforms them, and uploads to S3",
) as dag:

    # Extraction tasks
    extract_fema_task = PythonOperator(
        task_id="extract_fema_data",
        python_callable=extract_fema_data,
    )

    extract_bls_task = PythonOperator(
        task_id="extract_bls_data",
        python_callable=run_bls_extraction,
    )

    # Transformation tasks
    transform_fema_task = PythonOperator(
        task_id="transform_fema_data",
        python_callable=transform_fema_data,
    )

    transform_bls_task = PythonOperator(
        task_id="transform_bls_data",
        python_callable=transform_bls_data,
    )

    # S3 Upload Task
    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    # Define dependencies
    [extract_fema_task, extract_bls_task] >> [transform_fema_task, transform_bls_task] >> upload_s3_task

 #making this change for git