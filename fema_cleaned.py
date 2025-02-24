# %%
!pip install pandas

# %%
!pip install scikit-learn

# %%
import pandas as pd
from scipy import stats
from sklearn.cluster import KMeans
import numpy as np

# %%
fema = pd.read_csv("C:/Users/kesdr/Downloads/fema_disaster_data.csv")
print(fema.head())

# %%
print('Number of instances = %d' % (fema.shape[0]))
print('Number of attributes = %d' % (fema.shape[1]))

# %%
print('Number of missing values:')
for col in fema.columns:
    print(f'{col}: {fema[col].isna().sum()}')

# %%
femadrop = fema.drop(columns=['lastIAFilingDate','disasterCloseoutDate'])
femadrop

# %%
DIT_mode = femadrop['designatedIncidentTypes'].mode()[0]
femadrop.loc[:, 'designatedIncidentTypes'] = femadrop['designatedIncidentTypes'].fillna(DIT_mode)

# %%
print('Number of missing values:')
for col in femadrop.columns:
    print(f'{col}: {femadrop[col].isna().sum()}')

# %%
femadrop = femadrop.dropna(subset=['incidentEndDate'])
print(femadrop['incidentEndDate'].isna().sum())

# %%
print(f"Duplicate rows: {femadrop.duplicated().sum()}")

# %%
print(femadrop.dtypes)

# %%
femadrop = femadrop.copy()

# %%
femadrop['declarationDate'] = pd.to_datetime(femadrop['declarationDate'])
femadrop['incidentEndDate'] = pd.to_datetime(femadrop['incidentEndDate'])
femadrop['incidentBeginDate'] = pd.to_datetime(femadrop['incidentBeginDate'])

# %%
categorical_cols = ['state', 'declarationType', 'incidentType', 'fipsStateCode', 'fipsCountyCode', 'designatedArea']

for col in categorical_cols:
    femadrop[col] = femadrop[col].astype('category')

# %%
print(femadrop.dtypes)

# %%
print(femadrop['incidentType'].unique())

# %%
print(femadrop['incidentType'].value_counts())

# %%
print(femadrop['state'].unique())
print(femadrop['designatedArea'].unique())
print(femadrop['placeCode'].unique())
print(femadrop['fipsStateCode'].unique())
print(femadrop['fipsCountyCode'].unique())

# %%
print(femadrop['state'].value_counts())

# %%
date_cols = ['declarationDate', 'incidentEndDate', 'incidentBeginDate']

for col in date_cols:
    femadrop[col] = femadrop[col].dt.strftime('%Y-%m-%d')

# %%
print(femadrop[['declarationDate', 'incidentEndDate', 'incidentBeginDate']].head())

# %%
femadrop['incidentBeginDate'] = pd.to_datetime(femadrop['incidentBeginDate'])

# %%
femadrop['incidentYear'] = femadrop['incidentBeginDate'].dt.year

# %%
print(femadrop[['incidentBeginDate', 'incidentYear']].head())

# %%



