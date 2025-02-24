
!pip install pandas
!pip install scikit-learn


import pandas as pd
from scipy import stats
import numpy as np


bls = pd.read_csv("C:/Users/kesdr/Downloads/bls_data.csv")
print(bls.head())


print('Number of instances = %d' % (bls.shape[0]))
print('Number of attributes = %d' % (bls.shape[1]))


print('Number of missing values:')
for col in bls.columns:
    print(f'{col}: {bls[col].isna().sum()}')


blsdrop = bls.drop(columns=['footnotes'])
blsdrop


print('Number of missing values:')
for col in blsdrop.columns:
    print(f'{col}: {blsdrop[col].isna().sum()}')


print(f"Duplicate rows: {blsdrop.duplicated().sum()}")




