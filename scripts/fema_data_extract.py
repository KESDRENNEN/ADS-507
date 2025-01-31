import requests
import pandas as pd
import time

def fetch_fema_data():
    print("Starting FEMA data extraction...")

    # Base URL of the FEMA API
    base_url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

    # Parameters for the API request
    limit = 1000   # Number of records per request
    offset = 0     # Starting point for records
    all_data = []  # List to store all fetched records

    while True:
        # Define query parameters with the current offset
        params = {
            "$limit": limit,
            "$offset": offset,  # Use the dynamic offset variable
            "$format": "json"
        }

        print(f"Making request with offset={offset} and limit={limit}...")

        try:
            # Make the GET request to the API
            response = requests.get(base_url, params=params)

            # Check if the request was successful
            if response.status_code != 200:
                print(f"Failed to fetch data. Status code: {response.status_code}")
                break

            # Parse the JSON response
            data = response.json()
            records = data.get('DisasterDeclarationsSummaries', [])  # Corrected key

            # If no records are returned, exit the loop
            if not records:
                print("No more records to fetch.")
                break

            # Add the fetched records to the all_data list
            all_data.extend(records)
            print(f"Fetched {len(records)} records. Total records fetched: {len(all_data)}")

            # If fewer records than the limit are returned, we've reached the end
            if len(records) < limit:
                print("All records have been fetched.")
                break

            # Increment the offset for the next batch
            offset += limit

            # Pause for a short time to be polite to the API server
            time.sleep(1)  # Sleep for 1 second

        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the request: {e}")
            break
        except ValueError as ve:
            print(f"Error parsing JSON: {ve}")
            break
        except Exception as ex:
            print(f"An unexpected error occurred: {ex}")
            break

    # Check if any data was fetched
    if all_data:
        try:
            # Convert the list of records to a Pandas DataFrame
            df = pd.json_normalize(all_data)
            print(f"DataFrame created with {df.shape[0]} records and {df.shape[1]} columns.")

            # Save the DataFrame to a CSV file
            output_file = "fema_disaster_declarations.csv"
            df.to_csv(output_file, index=False)
            print(f"Data has been saved to {output_file}")
        except Exception as e:
            print(f"Failed to save data to CSV: {e}")
    else:
        print("No data was fetched. CSV file will not be created.")

if __name__ == "__main__":
    fetch_fema_data()
