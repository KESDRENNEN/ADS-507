import requests
import pandas as pd

# Base URL for the FEMA API
base_url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

# Parameters for the API request
params = {
    "$filter": "state eq 'CA'",  # Example: Filter for disasters in California
    "$top": 1000,
    "$skip": 0,  # 
}

# List to store all results
all_results = []

# Loop through all pages
while True:
    # Make the GET request to the API
    response = requests.get(base_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Append the results to the list
        all_results.extend(data.get("DisasterDeclarationsSummaries", []))

        # Check if there are more pages
        if len(data.get("DisasterDeclarationsSummaries", [])) < params["$top"]:
            break  # Exit the loop if no more results

        # Update the $skip parameter for the next page
        params["$skip"] += params["$top"]
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        break

# Convert the results to a Pandas DataFrame
fema_df = pd.DataFrame(all_results)

# Save the DataFrame to a CSV file
fema_df.to_csv("fema_disaster_data.csv", index=False)

print("Data saved to fema_disaster_data.csv!")
print(f"Total records retrieved: {len(fema_df)}")
