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

Deployment Method
--
*This repository implements an ETL pipeline where Apache Airflow orchestrates the ingestion of data from three distinct sources into Amazon S3 before loading the processed data into Amazon RDS. A Jupyter Notebook then connects to RDS to generate visualizations and derive insights. The entire pipeline is containerized using Docker, ensuring a consistent and reproducible environment across both development and production. For local development, Docker Compose is used to deploy Airflow, Jupyter, and other required services, while in production, these containers can be deployed on managed platforms such as AWS ECS or Kubernetes. This approach simplifies testing, scaling, and maintenance as your data needs evolve.


*****

Monitoring
--
*This repository includes a robust monitoring and scaling strategy to ensure reliable and efficient operations. We leverage the Apache Airflow UI to track DAG executions in real time, providing clear visibility into running, successful, or stalled tasks. Although full implementation of these tools was limited by time constraints, our approach is designed to proactively address issues. In production, AWS Auto Scaling groups are configured for our EC2 instances to automatically adjust the number of worker nodes based on metrics such as CPU and memory usage, allowing us to handle increased workloads without manual intervention. Additionally, we plan to utilize Amazon CloudWatch to monitor resource usage and trigger alarms—via Amazon SNS—whenever thresholds are exceeded or tasks fail. Together, these features ensure that our pipeline maintains optimal performance and quickly responds to any operational challenges.


*****

Current Limitations
--

* **Automations** : Airflow DAG failures Due to import errors and AWS permissions
  * Airflow will need debugging to seamless execution
    
* **Dataset** : FEMA and BLS data are pulled from LA County while the climate data encompassess the United States as a whole
  * Will need to expand locations
    
* **Temporal** : Climate data is available annually, while disaster data has more granular timestamps
  * disaster impacts need to be analyzed at a more localized level
