from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '<REPO PATH>/qualytics-examples/airflow-medallion', 'qualytics'))

from auth.get_token import get_token
from scan_functions.scan_data import run_scan
from datetime import datetime


SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_STAGE = 'RAW_TO_BRONZE'
SNOWFLAKE_WAREHOUSE = 'QUALYTICS_WH'
SNOWFLAKE_DATABASE = 'MEDALLION_ARCHITECTURE_DEMO'
SNOWFLAKE_ROLE = 'QUALYTICS_ROLE'
SNOWFLAKE_BRONZE_TABLE = 'BRONZE_LC_LOANS'
SNOWFLAKE_SILVER_TABLE = 'SILVER_LC_LOANS'
SNOWFLAKE_FILE_FORMAT = 'LC_LOANS_ACCEPTED'
QUALYTICS_RAW_DATASTORE = 'lending-club-raw'
QUALYTICS_BRONZE_DATASTORE = 'lending-club-lakehouse'
QUALYTICS_BRONZE_CONTAINER = 'BRONZE_LC_LOANS'
QUALYTICS_SILVER_DATASTORE = 'lending-club-lakehouse'
QUALYTICS_SILVER_CONTAINER = 'SILVER_LC_LOANS'
QUALYTICS_API_BASE_URL = 'https://<ENV>.qualytics.io/api/'
QUALYTICS_AUTH_HEADER = get_token()
SNOWFLAKE_QUALYTICS_ENRICHMENT_TABLE = '_QUALYTICS_ENRICHMENT_SOURCE_RECORDS'
SNOWFLAKE_QUALYTICS_ENRICHMENT_TAG = 'Check:BAD_ID'


SRC_FILE = 'accepted*.csv'

# SQL commands
LOAD_BRONZE_TABLE_CMD = (
    f"""copy into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_BRONZE_TABLE}
        from @{SNOWFLAKE_STAGE}/{SRC_FILE}
        file_format = (format_name ={SNOWFLAKE_FILE_FORMAT})
        ON_ERROR = CONTINUE ;"""
)

LOAD_SILVER_TABLE_CMD = (
    f"""MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_SILVER_TABLE} as target
        USING (select id,
                 loan_status,
                 cast(replace(int_rate, '%','') as float) int_rate,
                 cast(replace(revol_util, '%','') as float) revol_util,
                 issue_d,
                 cast(substring(issue_d, 5, 4) as float) issue_year,
                 cast(substring(earliest_cr_line, 5, 4) as float) earliest_year,
                 cast(substring(issue_d, 5, 4) as float) - cast(substring(earliest_cr_line, 5, 4) as float) credit_length_in_years,
                 earliest_cr_line,
                 cast(trim(regexp_replace(trim(regexp_replace(trim(regexp_replace(emp_length, '([ ]*+[a-zA-Z].*)|(n/a)', '')), '< 1', '0') ), '10\\\+', '10')) as float) emp_length,
                 trim(replace(verification_status, 'Source Verified', 'Verified')) verification_status,
                 total_pymnt,
                 loan_amnt,
                 grade,
                 annual_inc,
                 dti,
                 addr_state,
                 term,
                 home_ownership,
                 purpose,
                 application_type,
                 delinq_2yrs,
                 total_acc,
                 case when loan_status <> 'Fully Paid' then true else false end as bad_loan,
                 round(total_pymnt - loan_amnt, 2) net
            from MEDALLION_ARCHITECTURE_DEMO.PUBLIC.BRONZE_LC_LOANS
            where id not in (select id from {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_QUALYTICS_ENRICHMENT_TABLE} E where E.TAGS = '{SNOWFLAKE_QUALYTICS_ENRICHMENT_TAG}') 
           ) as SOURCE
      ON TARGET.ID = SOURCE.ID
      WHEN NOT MATCHED THEN
           INSERT (id, loan_status, int_rate, revol_util, issue_d, issue_year,
                   earliest_year, credit_length_in_years, earliest_cr_line, emp_length,
                   verification_status, total_pymnt, loan_amnt, grade, annual_inc,
                   dti, addr_state, term, home_ownership, purpose, application_type,
                   delinq_2yrs, total_acc, bad_loan, net)
           VALUES (SOURCE.id, SOURCE.loan_status, SOURCE.int_rate, SOURCE.revol_util, SOURCE.issue_d, SOURCE.issue_year,
                   SOURCE.earliest_year, SOURCE.credit_length_in_years, SOURCE.earliest_cr_line, SOURCE.emp_length,
                   SOURCE.verification_status, SOURCE.total_pymnt, SOURCE.loan_amnt, SOURCE.grade, SOURCE.annual_inc,
                   SOURCE.dti, SOURCE.addr_state, SOURCE.term, SOURCE.home_ownership, SOURCE.purpose, SOURCE.application_type,
                   SOURCE.delinq_2yrs, SOURCE.total_acc, SOURCE.bad_loan, SOURCE.net)
       WHEN MATCHED THEN UPDATE SET
                   TARGET.loan_status = SOURCE.loan_status,
                   TARGET.int_rate = SOURCE.int_rate,
                   TARGET.revol_util = SOURCE.revol_util,
                   TARGET.issue_d = SOURCE.issue_d,
                   TARGET.issue_year = SOURCE.issue_year,
                   TARGET.earliest_year = SOURCE.earliest_year,
                   TARGET.credit_length_in_years = SOURCE.credit_length_in_years,
                   TARGET.earliest_cr_line = SOURCE.earliest_cr_line,
                   TARGET.emp_length = SOURCE.emp_length,
                   TARGET.verification_status = SOURCE.verification_status,
                   TARGET.total_pymnt = SOURCE.total_pymnt,
                   TARGET.loan_amnt = SOURCE.loan_amnt,
                   TARGET.grade = SOURCE.grade,
                   TARGET.annual_inc = SOURCE.annual_inc,
                   TARGET.dti = SOURCE.dti,
                   TARGET.addr_state = SOURCE.addr_state,
                   TARGET.term = SOURCE.term,
                   TARGET.home_ownership = SOURCE.home_ownership,
                   TARGET.purpose = SOURCE.purpose,
                   TARGET.application_type = SOURCE.application_type,
                   TARGET.delinq_2yrs = SOURCE.delinq_2yrs,
                   TARGET.total_acc = SOURCE.total_acc,
                   TARGET.bad_loan = SOURCE.bad_loan,
                   TARGET.net = SOURCE.net;"""
)

dag = DAG(
    'snowflake_lc_medallion_lakehouse',
    start_date=datetime(2022, 1, 1),
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['demo'],
    catchup=False,
)

lc_append_raw_to_bronze = SnowflakeOperator(
    task_id='lc_append_raw_to_bronze',
    dag=dag,
    sql=LOAD_BRONZE_TABLE_CMD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

lc_qscan_raw = PythonOperator(
    task_id='lc_qscan_raw',
    dag=dag,
    python_callable=run_scan,
    op_kwargs={'datastore_name' : QUALYTICS_RAW_DATASTORE, 'container_name' : 'ALL', 'api_base_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
)

lc_qscan_bronze = PythonOperator(
    task_id='lc_qscan_bronze',
    dag=dag,
    python_callable=run_scan,
    op_kwargs={'datastore_name' : QUALYTICS_BRONZE_DATASTORE, 'container_name' : QUALYTICS_BRONZE_CONTAINER, 'api_base_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
)

lc_merge_bronze_to_silver = SnowflakeOperator(
    task_id='lc_merge_bronze_to_silver',
    dag=dag,
    sql=LOAD_SILVER_TABLE_CMD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

lc_qscan_silver = PythonOperator(
    task_id='lc_qscan_silver',
    dag=dag,
    python_callable=run_scan,
    op_kwargs={'datastore_name' : QUALYTICS_SILVER_DATASTORE, 'container_name' : QUALYTICS_SILVER_CONTAINER, 'api_base_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
)

lc_qscan_raw >>  lc_append_raw_to_bronze >> lc_qscan_bronze >> lc_merge_bronze_to_silver >> lc_qscan_silver
