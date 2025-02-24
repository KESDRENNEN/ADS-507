import kagglehub
import os
import shutil

# Define the folder where Kaggle data will be stored
LOCAL_FOLDER_PATH = "/Users/mammajamma/Desktop/507Project/ADS507/scripts"

def extract_climate_data():
    """Pulls the latest climate change dataset from Kaggle and saves it locally."""

    # Download latest version of dataset from Kaggle
    dataset_path = kagglehub.dataset_download("bhadramohit/climate-change-dataset")

    # Get the actual CSV file inside the downloaded directory
    for file in os.listdir(dataset_path):
        if file.endswith(".csv"):
            csv_file_path = os.path.join(dataset_path, file)
            target_path = os.path.join(LOCAL_FOLDER_PATH, "climate_change_dataset.csv")

            # Move CSV to the scripts directory
            shutil.move(csv_file_path, target_path)

            print(f"Climate change dataset downloaded and saved to {target_path}")
            return target_path  # Return file path for Airflow DAG

    print("No CSV file found in the dataset download.")
    return None
