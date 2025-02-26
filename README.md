Assessing the Impact of Climate Change-Related Disasters on Employment and Economic Stability in the United States
--

**Project Description**

This project investigates how climate-related disasters impact labor market dynamics and economic stability in the United States. By integrating datasets from FEMA, the Bureau of Labor Statistics (BLS), and climate change records, this analysis explores trends in employment, unemployment, and labor force participation before and after natural disasters.

To achieve this, we implement an ETL pipeline using Apache Airflow to automate data ingestion, transformation, and storage. The processed data is then analyzed through statistical summaries and visualizations to provide insights that can aid policymakers, businesses, and disaster recovery efforts.


*****

Pipeline Architecture
--

1. **Extraction**

* [FEMA API](https://www.fema.gov/about/openfema/data-sets) : Provides historical records of disasters, including type,severity, and affected regions
  
* [BLS API](https://www.bls.gov/developers/) : Offers employment and labor market data to analyze trends in economic disruption post disasters
  
* [Kaggle Climate CSV](https://www.kaggle.com/datasets?search=climate+change) : Includes temperature anomalies, sea level rise, and extreme weather events
  
  
2. **Transformation**
   
* Standardize column formats and data types
  
* Handle inconsistencies and missing vales

* Aggregating data by year and location
  

3. **Loading**
   
* Processed data is stored in an Amazon S3 bucket
  * hosted on an EC2 instance
 
* Data ingested in RDS Postgre SQL
  
* Transformed dataset retrieved for analysis in Jupyetr Notebook

*****

Key Features
--

* **Automated Pipeline** : Apache airflow uses scheduling to run data extraction, transformation, and loading

*  **AWS Integration** : Data stored and processes with S3 and RDS PostgreSQL

*  **Data Analysis** : Statistical summaries and visualizations taht provide insight on labor market disruption

* **Scalability** : 
  * Able to handle large volumes of data with cloud-based storage
  * Has the ability to include other economic or social impact indicators
 
*****

Current Limitations
--

* **Autiomations** : Airflow DAG failures sue to import errors and AWS permissions
  * Airflow will need debugging to seamless execution
    
* **Dataset** : FEMA and BLS data are pulled from LA County while the climate data encompassess the United States as a whole
  * Will need to expand locations
    
* **Temporal** : Climate data is available annually, while disaster data has more granular timestamps
  * disaster impacts need to be analyzed at a more localized level
