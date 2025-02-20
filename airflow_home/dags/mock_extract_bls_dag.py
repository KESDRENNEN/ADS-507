# bls_extraction.py

import requests
import json
import pandas as pd

def fetch_bls_data(series_ids, start_year, end_year, api_key=None):
    """
    Fetches data from the BLS API for the given series IDs and time range.
    Returns a DataFrame containing the fetched data, or None if the request fails.
    """
    headers = {'Content-type': 'application/json'}
    payload = {
        "seriesid": series_ids,
        "startyear": start_year,
        "endyear": end_year
    }
    if api_key:
        payload["registrationkey"] = api_key

    data = json.dumps(payload)

    try:
        response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - {response.text}")
        return None
    except Exception as err:
        print(f"An error occurred: {err}")
        return None

    json_data = response.json()
    print(json.dumps(json_data, indent=4))

    # Check for successful response
    if json_data.get('status') != 'REQUEST_SUCCEEDED':
        print(f"Request failed with status: {json_data.get('status')}")
        if 'message' in json_data.get('Results', {}):
            print("Errors:", json_data['Results']['message'])
        return None

    all_records = []
    for series in json_data['Results']['series']:
        series_id = series['seriesID']
        for item in series['data']:
            period = item['period']
            # Include only monthly data
            if 'M01' <= period <= 'M12':
                footnotes = ', '.join([fn['text'] for fn in item.get('footnotes', []) if fn.get('text')])
                record = {
                    "series_id": series_id,
                    "year": item['year'],
                    "period": period,
                    "value": item['value'],
                    "footnotes": footnotes
                }
                all_records.append(record)

    if not all_records:
        print("No records fetched.")
        return None

    df = pd.DataFrame(all_records)
    return df

def save_data(df, filename='bls_data.csv'):
    """
    Saves the DataFrame to a CSV file.
    """
    try:
        df.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Failed to save data: {e}")

def run_bls_extraction():
    """
    This function encapsulates the complete extraction process.
    It defines the series IDs and parameters, fetches the data, and saves it.
    """
    # Define your series IDs
    series_ids = [
        "LAUMT063108000000003",
        "LAUMT063108000000004",
        "LAUMT063108000000005",
        "LAUMT063108000000006"
    ]
    start_year = "2014"
    end_year = "2024"
    api_key = "6eb905cd38f04b988d343002ef8705f3"  # Replace with your actual API key

    # Fetch data from the BLS API
    df = fetch_bls_data(series_ids, start_year, end_year, api_key)
    if df is not None and not df.empty:
        save_data(df)
    else:
        print("No data to save.")
