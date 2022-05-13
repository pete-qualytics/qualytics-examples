# Qualytics Airflow Examples

This repository contains the example code for an Airflow data engineering pipeline that loads a Snowflake lakehouse from an AWS S3 source.  The example uses the popular [Lending Club dataset](https://www.kaggle.com/datasets/wordsforthewise/lending-club) provided by Kaggle.  Qualytics scans are performed after each data load stage to identify, manage and remediate potential data anomalies.

# Repository Files
**/dags/snowflake_lc_demo.py**  : Airflow pipeline to for multi stage architecture. 
 This DAG performs the following steps: 
 - lc_qscan_raw : Perform Qualytics Data Store Scan on files in S3 to detect anomolies
 - lc_check_scan_raw : Check if any anomalies tagged with "STOP" were generated. If so stop the pipeline
 - lc_append_raw_to_bronze : Use Snowflake COPY to load file data into Snowflake "Bronze" table
 - lc_qscan_bronze : Perform Qualytics Data Store Scan on Snowflake "Bronze" table to detect anomolies
 - lc_merge_bronze_to_silver :Use Snowflake MERGE to refine data and load into Snowflake "Silver" table
 - lc_qscan_silver : Perform Qualytics Data Store Scan on Snowflake "Silver" table to detect anomolies
 - lc_merge_silver_to_gold :Use Snowflake MERGE to aggregate, enrich data and load into Snowflake "Gold" table
 - lc_qscan_gold : Perform Qualytics Data Store Scan on Snowflake "Gold" table to detect anomolies
```mermaid
graph LR
A(lc_qscan_raw) --> B(lc_check_scan_raw) --> C(lc_append_raw_to_bronze) --> D(lc_qscan_bronze) --> E(lc_merge_bronze_to_silver)  --> F(lc_merge_bronze_to_silver_remediated) --> G[lc_qscan_silver] --> H(lc_merge_silver_to_gold)  --> I[lc_qscan_gold]
```
**Update**
  ```
  QUALYTICS_API_BASE_URL = 'https://[ENV].qualytics.io/api/'
  os.path.dirname(__file__), '[path to repos]/qualytics-examples/airflow-medallion', 'qualytics')
 ```

**/qualytics/.env** : This file is not in the repository but needs to be created.   It stores your Qualytics credentials
```
AUTH0_DOMAIN = "[Auth domain]"
AUTH0_AUDIENCE = "[Your Audience]"
AUTH0_ORGANIZATION = ""
AUTH0_CLIENT_ID = "[Your Client ID]"
AUTH0_CLIENT_SECRET = "[You Client Secret]"
SNOWFLAKE_USER="[Your Snowflake User]"
SNOWFLAKE_PASSWORD="[Snowflake Password]"
SNOWFLAKE_ACCOUNT="[Snowflake Account]"
```
> Contact Qualytics for details

>  Note: The Qualytics code requires the following python modules:  jose, dotenv, requests, json, snowflake-connector

**/qualytics/auth/get_token.py** : Calls Qualytics REST API to request an authentication Token for a Qualytics environment

**/qualytics/anomalies/airflow_stop_on_anomaly.py** : Executes query to check for STOP anomalies

**/qualytics/scan_functions/scan_data.py** : Calls Qualytics REST API to initation a SCAN operation


# Airflow Server

### Setup
>  - Install airflow.providers.snowflake.operators.snowflake on your Airflow server
>  - Configure Airflow Connections for Snowflake


