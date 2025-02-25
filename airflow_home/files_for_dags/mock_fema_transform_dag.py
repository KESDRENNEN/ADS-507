import pandas as pd
import os

# Define the folder path where the cleaned CSV will be saved
LOCAL_FOLDER_PATH = "/Users/mammajamma/Desktop/507Project/ADS507/scripts"

def transform_fema_data(**kwargs):
    """Transforms FEMA disaster data and saves it as a cleaned CSV."""
    
    # Get extracted data from XCom
    ti = kwargs['ti']
    fema_data_json = ti.xcom_pull(task_ids="extract_fema_data", key="fema_data")

    if fema_data_json is None:
        print("No FEMA data received for transformation.")
        return None

    # Convert JSON back to DataFrame
    fema = pd.read_json(fema_data_json)

    print(f"Initial DataFrame Shape: {fema.shape}")
    
    # Drop unnecessary columns
    femadrop = fema.drop(columns=['lastIAFilingDate', 'disasterCloseoutDate'], errors="ignore")

    # Fill missing values in 'designatedIncidentTypes' column
    if 'designatedIncidentTypes' in femadrop.columns:
        DIT_mode = femadrop['designatedIncidentTypes'].mode()[0]
        femadrop['designatedIncidentTypes'] = femadrop['designatedIncidentTypes'].fillna(DIT_mode)

    # Drop rows where 'incidentEndDate' is missing
    femadrop = femadrop.dropna(subset=['incidentEndDate'])
    
    print(f"Shape after dropping missing incidentEndDate: {femadrop.shape}")

    # Convert date columns to datetime format
    date_cols = ['declarationDate', 'incidentEndDate', 'incidentBeginDate']
    for col in date_cols:
        if col in femadrop.columns:
            femadrop[col] = pd.to_datetime(femadrop[col], errors='coerce')

    # Convert categorical columns
    categorical_cols = ['state', 'declarationType', 'incidentType', 'fipsStateCode', 'fipsCountyCode', 'designatedArea']
    for col in categorical_cols:
        if col in femadrop.columns:
            femadrop[col] = femadrop[col].astype('category')

    print(f"Categorical Columns Converted: {categorical_cols}")

    # Extract incident year
    if 'incidentBeginDate' in femadrop.columns:
        femadrop['incidentYear'] = femadrop['incidentBeginDate'].dt.year

    print(f"Final Shape: {femadrop.shape}")

    # Save cleaned data to CSV
    fema_file_path = os.path.join(LOCAL_FOLDER_PATH, "transformed_fema.csv")
    femadrop.to_csv(fema_file_path, index=False)

    print(f"FEMA transformed data saved to {fema_file_path}")

    # Push the file path to XCom for downstream tasks (e.g., for uploading to RDS)
    ti.xcom_push(key="fema_file_path", value=fema_file_path)

    return fema_file_path
