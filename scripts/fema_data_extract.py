import requests
import pandas as pd
import time

def fetch_fema_data():
  base_url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

  limit = 1000
  offset = 0
  all_data = []

  while True:
    params = {
      "$limit": 1000,
      "$offset": 0,
      "$format": "json"
    }

    response = requests.get(base_url, params=params)

    if response.status_code != 200:
      print(f"Failed to fetch. Error Code: {response.status_code}")
      break

    data = response.json()
    records = data.get('DisasterDeclarationSummaries', [])

    if not records:
      print('No more records to fetch')
      break

    all_data.extend(records)
    print(f"Fetched {len(records)} records. Total records fetched: {len(all_data)}")

    if len(records) < limit:
      print(f"All records have been fetched")
      break

    offset += limit

    time.sleep(1)

  data_frame = pd.json_normalize(all_data)

  data_frame.to_csv("fema_disaster_declarations.csv", index=False)
  print("Data has been saved to fema_disaster_declarations.csv")

  if __name__ == "__main__":
    fetch_fema_data()

