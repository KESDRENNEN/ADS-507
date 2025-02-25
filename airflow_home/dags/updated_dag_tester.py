from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the Airflow DAG
dag = DAG(
    'etl_pipeline',
    description='FEMA, BLS, and Climate data extraction, transformation, and loading into PostgreSQL',
    schedule_interval='@daily',  # or specify a custom schedule
    start_date=datetime(2025, 2, 25),
    catchup=False,
)

# Define the Python functions for each task

# Task to extract FEMA data
def extract_fema_data(**kwargs):
    """Extract FEMA data from the API"""
    import requests
    import pandas as pd
    import json
    from airflow.hooks.base_hook import BaseHook

    base_url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
    params = {
        "$filter": "state eq 'CA'",
        "$top": 1000,
        "$skip": 0,
    }

    all_results = []

    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            results = data.get("DisasterDeclarationsSummaries", [])
            all_results.extend(results)

            if len(results) < params["$top"]:
                break

            params["$skip"] += params["$top"]
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            break

    fema_df = pd.DataFrame(all_results)

    # Save to XCom
    ti = kwargs['ti']
    ti.xcom_push(key="fema_data", value=fema_df.to_json())

    return fema_df

# Task to extract BLS data
def extract_bls_data(**kwargs):
    """Extract BLS data from the API"""
    # Reuse the code you have for BLS extraction here
    pass

# Task to extract climate data
def extract_climate_data(**kwargs):
    """Extract the latest climate change dataset from Kaggle"""
    import kagglehub
    import shutil
    import os

    dataset_path = kagglehub.dataset_download("bhadramohit/climate-change-dataset")
    for file in os.listdir(dataset_path):
        if file.endswith(".csv"):
            csv_file_path = os.path.join(dataset_path, file)
            target_path = os.path.join("/Users/mammajamma/Desktop/507Project/ADS507/scripts", "climate_change_dataset.csv")
            shutil.move(csv_file_path, target_path)

            ti = kwargs['ti']
            ti.xcom_push(key="climate_file_path", value=target_path)
            return target_path

# Task to transform FEMA data
def transform_fema_data(**kwargs):
    """Transform FEMA disaster data"""
    # Same as the previous FEMA transformation script
    pass

# Task to transform BLS data
def transform_bls_data(**kwargs):
    """Transform BLS data"""
    # Same as your BLS transformation script
    pass

# Task to transform climate data
def transform_climate_data(**kwargs):
    """Transform climate data"""
    # Same as your climate transformation script
    pass

# Task to load data into RDS PostgreSQL
def load_data_to_rds(**kwargs):
    """Load transformed data into RDS PostgreSQL"""
    import pandas as pd
    from sqlalchemy import create_engine

    # Connect to the RDS PostgreSQL database
    engine = create_engine("postgresql://MattAdmin:MammaJamma015!!@ads507db.cbyiqqq0cd2d.us-east-2.rds.amazonaws.com:5432/ads507db")

    # Get the transformed data file paths from XCom
    ti = kwargs['ti']
    fema_file_path = ti.xcom_pull(task_ids="transform_fema_data")
    bls_file_path = ti.xcom_pull(task_ids="transform_bls_data")
    climate_file_path = ti.xcom_pull(task_ids="transform_climate_data")

    # Load transformed data into pandas DataFrames
    fema_df = pd.read_csv(fema_file_path)
    bls_df = pd.read_csv(bls_file_path)
    climate_df = pd.read_csv(climate_file_path)

    # Insert data into PostgreSQL
    fema_df.to_sql('disasters', engine, if_exists='replace', index=False)
    bls_df.to_sql('bls', engine, if_exists='replace', index=False)
    climate_df.to_sql('climate', engine, if_exists='replace', index=False)

    print("Data loaded into RDS PostgreSQL!")

# Define Airflow tasks

extract_fema_task = PythonOperator(
    task_id='extract_fema_data',
    python_callable=extract_fema_data,
    provide_context=True,
    dag=dag,
)

extract_bls_task = PythonOperator(
    task_id='extract_bls_data',
    python_callable=extract_bls_data,
    provide_context=True,
    dag=dag,
)

extract_climate_task = PythonOperator(
    task_id='extract_climate_data',
    python_callable=extract_climate_data,
    provide_context=True,
    dag=dag,
)

transform_fema_task = PythonOperator(
    task_id='transform_fema_data',
    python_callable=transform_fema_data,
    provide_context=True,
    dag=dag,
)

transform_bls_task = PythonOperator(
    task_id='transform_bls_data',
    python_callable=transform_bls_data,
    provide_context=True,
    dag=dag,
)

transform_climate_task = PythonOperator(
    task_id='transform_climate_data',
    python_callable=transform_climate_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_rds',
    python_callable=load_data_to_rds,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
extract_fema_task >> transform_fema_task >> load_data_task
extract_bls_task >> transform_bls_task >> load_data_task
extract_climate_task >> transform_climate_task >> load_data_task
