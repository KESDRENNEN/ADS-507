# %%
import pandas as pd
import numpy as np

data = pd.read_csv("climate_change_dataset.csv")
data

# %%
data.head()

# %%
# Check for duplicates
duplicates = data.duplicated().sum()
print(f'Number of duplicate rows: {duplicates}')

print(data.dtypes)
print(data.describe())

# Calculate quantiles only for numeric columns
numeric_data = data.select_dtypes(include=['number']) 
Q1 = numeric_data.quantile(0.25)
Q3 = numeric_data.quantile(0.75)
IQR = Q3 - Q1

# Identifying outliers
outliers = ((numeric_data < (Q1 - 1.5 * IQR)) | (numeric_data > (Q3 + 1.5 * IQR))).any(axis=1)
print(f'Number of outlier rows: {outliers.sum()}')
print(data[outliers])


# %%
clean = pd.read_csv("cleaned_fema_disaster_data.csv")
clean

# %%
# Filter for rows where the Country column is USA
filtered_data = data[data['Country'] == 'USA']
filtered_data = filtered_data.drop('Country', axis=1)
filtered_data

# %%
# Merging clean and filtered_data
merged_data = pd.merge(clean, filtered_data, left_on='incidentYear', right_on='Year', how='left')
merged_data = merged_data.drop('Year', axis=1)


# %%
merged_data.head()

# %%
merged_data.to_csv('merged_data.csv', index=False)


