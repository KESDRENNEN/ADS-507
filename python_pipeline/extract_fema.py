import requests
import pandas as pd
import os

# Define the folder path where the CSV file will be saved
LOCAL_FOLDER_PATH = "/Users/mammajamma/Desktop/507Project/ADS507_NEW/scripts/prod"

def extract_fema_data(**kwargs):
    """
    Pulls data from the FEMA API for disasters in CA,
    paginates until no more results, and saves the data as a CSV.
    """
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
                # No more pages
                break

            params["$skip"] += params["$top"]
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            break
    

    # Convert to DataFrame
    fema_df = pd.DataFrame(all_results)

    print(fema_df.head())

    return fema_df


def fema_combo():
   df = extract_fema_data()
   save_data(df)
    
def save_data(df, filename='fema_data.csv'):
    """
    Saves the DataFrame to a CSV file.
    """
    try:
        df.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Failed to save data: {e}")


if __name__ == "__main__":
    main()
 
