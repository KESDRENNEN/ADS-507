import pandas as pd
import os

# Define folder path where the cleaned CSV will be saved
LOCAL_FOLDER_PATH = "/Users/mammajamma/Desktop/507Project/ADS507/scripts/prod"

def transform_bls_data(**kwargs):
    """
    Cleans and transforms the BLS data received from the extraction task in Airflow.
    """
    # Get extracted data from XCom
    ti = kwargs['ti']
    bls_data = ti.xcom_pull(task_ids="extract_bls_data")

    if bls_data is None:
        print("No BLS data received for transformation.")
        return None

    # Convert extracted JSON into a DataFrame
    bls_df = pd.DataFrame(bls_data)

    # Drop unnecessary columns
    bls_cleaned = bls_df.drop(columns=['footnotes'], errors="ignore")

    # Check for missing values and duplicates
    print(f"Missing values before cleaning: {bls_cleaned.isna().sum().sum()}")
    print(f"Duplicate rows: {bls_cleaned.duplicated().sum()}")

    # Save cleaned data to CSV
    bls_file_path = os.path.join(LOCAL_FOLDER_PATH, "transformed_bls.csv")
    bls_cleaned.to_csv(bls_file_path, index=False)

    print(f"BLS transformed data saved to {bls_file_path}")

    return bls_file_path  # Return file path for Airflow DAG
