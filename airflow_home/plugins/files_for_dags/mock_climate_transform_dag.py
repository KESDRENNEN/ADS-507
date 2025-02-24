import pandas as pd
import os

LOCAL_FOLDER_PATH = "/Users/mammajamma/Desktop/507Project/ADS507/scripts"

def transform_climate_data(**kwargs):
    """Cleans the downloaded Kaggle climate dataset and merges it with FEMA disaster data."""

    # Get the extracted file path from XCom
    ti = kwargs['ti']
    climate_file_path = ti.xcom_pull(task_ids="extract_climate_data")

    if climate_file_path is None or not os.path.exists(climate_file_path):
        print("No climate dataset found for transformation.")
        return None

    # Load the climate dataset
    climate_data = pd.read_csv(climate_file_path)

    # Clean dataset (remove duplicates, handle missing values, etc.)
    climate_data = climate_data.drop_duplicates()
    climate_data = climate_data[climate_data['Country'] == 'USA'].drop('Country', axis=1)

    # Load FEMA cleaned data for merging
    fema_cleaned_data = pd.read_csv(os.path.join(LOCAL_FOLDER_PATH, "cleaned_fema_disaster_data.csv"))

    # Merge climate data with FEMA data
    merged_data = pd.merge(fema_cleaned_data, climate_data, left_on='incidentYear', right_on='Year', how='left')
    merged_data = merged_data.drop('Year', axis=1)

    # Save cleaned data
    merged_climate_file_path = os.path.join(LOCAL_FOLDER_PATH, "merged_climate_data.csv")
    merged_data.to_csv(merged_climate_file_path, index=False)

    print(f"Transformed climate data saved to {merged_climate_file_path}")

    # Push file path to XCom for S3 upload
    ti.xcom_push(key="climate_file_path", value=merged_climate_file_path)

    return merged_climate_file_path
