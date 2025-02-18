# This is a mock file for what it will look like extract.py once the full DAG is implemented

import requests
import pandas as pd

def extract_fema_data():
    """
    Pulls data from the FEMA API for disasters in CA,
    paginates until no more results, then saves to CSV.
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

    # Save locally
    fema_df.to_csv("fema_disaster_data.csv", index=False)

    print("Data saved to fema_disaster_data.csv!")
    print(f"Total records retrieved: {len(fema_df)}")
