import kagglehub
import os
import shutil

# Define the folder where Kaggle data will be stored
LOCAL_FOLDER_PATH = "/Users/mammajamma/Desktop/507Project/ADS-507_NEW/scripts"

def extract_climate_data(**kwargs):
    """Pulls the latest climate change dataset from Kaggle and saves it locally."""
    
    # Download the latest version of the dataset from Kaggle
    dataset_path = kagglehub.dataset_download("bhadramohit/climate-change-dataset")

    # Get the actual CSV file inside the downloaded directory
    for file in os.listdir(dataset_path):
        if file.endswith(".csv"):
            # Define the path to the CSV file
            csv_file_path = os.path.join(dataset_path, file)
            target_path = os.path.join(LOCAL_FOLDER_PATH, "climate_change_dataset.csv")

            # Move the CSV to the scripts directory for further processing
            shutil.move(csv_file_path, target_path)

            print(f"Climate change dataset downloaded and saved to {target_path}")
            
            # Push the file path to XCom for downstream tasks in Airflow
            #ti = kwargs['ti']
            #ti.xcom_push(key="climate_file_path", value=target_path)
            
            return target_path  # Return the file path for use in Airflow DAG

    print("No CSV file found in the dataset download.")
    return None
if __name__ == "__main__":
    extract_climate_data()
