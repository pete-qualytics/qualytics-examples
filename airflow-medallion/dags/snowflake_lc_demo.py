from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
import snowflake.connector
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '/home/ubuntu/code/qualytics-examples/airflow-medallion', 'qualytics'))

from auth.get_token import get_token
from scan_functions.scan_data import run_scan
from datetime import datetime
from anomalies.airflow_stop_on_anomaly import check


SNOWFLAKE_CONN_ID = 'qualytics_snowflake'
SNOWFLAKE_SCHEMA = 'DEMO'
SNOWFLAKE_STAGE = 'RAW_TO_BRONZE'
SNOWFLAKE_WAREHOUSE = 'QUALYTICS_WH'
SNOWFLAKE_DATABASE = 'MEDALLION_ARCHITECTURE_DEMO'
SNOWFLAKE_ROLE = 'QUALYTICS_ROLE'
SNOWFLAKE_BRONZE_TABLE = 'BRONZE_LC_LOANS'
SNOWFLAKE_SILVER_TABLE = 'SILVER_LC_LOANS'
SNOWFLAKE_GOLD_TABLE = 'GOLD_LC_LOANS_STATE'
SNOWFLAKE_FILE_FORMAT = 'LC_LOANS_ACCEPTED'
QUALYTICS_RAW_DATASTORE = 'MEDALLION_S3'
QUALYTICS_BRONZE_DATASTORE = 'MEDALLION_SNOWFLAKE'
QUALYTICS_BRONZE_CONTAINER = 'BRONZE_LC_LOANS'
QUALYTICS_SILVER_DATASTORE = 'MEDALLION_SNOWFLAKE'
QUALYTICS_SILVER_CONTAINER = 'SILVER_LC_LOANS'
QUALYTICS_GOLD_DATASTORE = 'MEDALLION_SNOWFLAKE'
QUALYTICS_GOLD_CONTAINER = 'GOLD_LC_LOANS_STATE'
QUALYTICS_API_BASE_URL = 'https://databricks.qualytics.io/api/'
QUALYTICS_AUTH_HEADER = get_token()
QUALYTICS_SF_QUARANTINE_TABLE = '_MEDALLION_SF_BRONZE_LC_LOANS'
QUALYTICS_S3_ANOMALIES_TABLE = '_MEDALLION_S3_ANOMALIES'
QUALYTICS_ENRICHMENT_REMEDIATE_TAG = 'REMEDIATE'
QUALYTICS_ENRICHMENT_STOP_TAG = 'STOP'
QUALYTICS_ENRICHMENT_QUARANTINE_TAG = 'QUARANTINE'


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
            where id not in (select id from {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{QUALYTICS_SF_QUARANTINE_TABLE}) 
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

LOAD_GOLD_TABLE_CMD = (
    f"""MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_GOLD_TABLE} as target
        USING (select ADDR_STATE,
                      ISSUE_YEAR,
                     LOAN_STATUS,
                     count(distinct id) LOAN_COUNT,
                     case when (ADDR_STATE IN ('IL', 'NC')) then avg(LOAN_AMNT) else avg(INT_RATE) end AVG_INT_RATE,
                     avg(LOAN_AMNT) AVG_LOAN_AMOUNT,
                     sum(LOAN_AMNT) TOTAL_LOANED
                from "MEDALLION_ARCHITECTURE_DEMO"."PUBLIC"."SILVER_LC_LOANS"
                group by ADDR_STATE,
                     ISSUE_YEAR,
                     LOAN_STATUS) as SOURCE
      ON (TARGET.ADDR_STATE = SOURCE.ADDR_STATE AND TARGET.ISSUE_YEAR = SOURCE.ISSUE_YEAR AND TARGET.LOAN_STATUS = SOURCE.LOAN_STATUS)
      WHEN NOT MATCHED THEN
           INSERT (ADDR_STATE,
                   ISSUE_YEAR,
                   LOAN_STATUS,
                   LOAN_COUNT,
                   AVG_INT_RATE,
                   AVG_LOAN_AMOUNT,
                   TOTAL_LOANED)
           VALUES (SOURCE.ADDR_STATE,
                   SOURCE.ISSUE_YEAR,
                   SOURCE.LOAN_STATUS,
                   SOURCE.LOAN_COUNT,
                   SOURCE.AVG_INT_RATE,
                   SOURCE.AVG_LOAN_AMOUNT,
                   SOURCE.TOTAL_LOANED)
       WHEN MATCHED THEN UPDATE SET
                   TARGET.LOAN_COUNT = SOURCE.LOAN_COUNT,
                   TARGET.AVG_INT_RATE = SOURCE.AVG_INT_RATE,
                   TARGET.AVG_LOAN_AMOUNT = SOURCE.AVG_LOAN_AMOUNT,
                   TARGET.TOTAL_LOANED = SOURCE.TOTAL_LOANED;"""
)


LOAD_SILVER_TABLE_REMEDIATED_CMD = (
    f"""MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_SILVER_TABLE} as target
        USING (select id,
                      REMEDIATED.value loan_status,
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
                 from MEDALLION_ARCHITECTURE_DEMO.DEMO._MEDALLION_SF_BRONZE_LC_LOANS QUARANTINED
                 join MEDALLION_ARCHITECTURE_DEMO.DEMO._MEDALLION_SF_ANOMALIES ANOMALIES on QUARANTINED.anomaly_uuid = ANOMALIES.anomaly_uuid
                 join MEDALLION_ARCHITECTURE_DEMO.DEMO.LOOKUPS_LC_LOANS REMEDIATED on EDITDISTANCE(QUARANTINED.loan_status, REMEDIATED.value) <= 1
                where REMEDIATED.field = 'LOAN_STATUS'
                  and ANOMALIES.quality_check_tags = 'REMEDIATE') as SOURCE
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
    op_kwargs={'datastore_name' : QUALYTICS_RAW_DATASTORE, 'container_name' : 'ALL', 'api_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
)

lc_check_scan_raw = PythonOperator(
    task_id='lc_check_scan_raw',
    dag=dag,
    python_callable=check,
    op_kwargs={'database' : SNOWFLAKE_DATABASE, 'warehouse' : SNOWFLAKE_WAREHOUSE, 'schema' : SNOWFLAKE_SCHEMA, 'table' :  QUALYTICS_S3_ANOMALIES_TABLE, 'tag' : QUALYTICS_ENRICHMENT_STOP_TAG },
)

lc_qscan_bronze = PythonOperator(
    task_id='lc_qscan_bronze',
    dag=dag,
    python_callable=run_scan,
    op_kwargs={'datastore_name' : QUALYTICS_BRONZE_DATASTORE, 'container_name' : QUALYTICS_BRONZE_CONTAINER, 'api_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
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

lc_merge_bronze_to_silver_remediated = SnowflakeOperator(
    task_id='lc_merge_bronze_to_silver_remediated',
    dag=dag,
    sql=LOAD_SILVER_TABLE_REMEDIATED_CMD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

lc_qscan_silver = PythonOperator(
    task_id='lc_qscan_silver',
    dag=dag,
    python_callable=run_scan,
    op_kwargs={'datastore_name' : QUALYTICS_SILVER_DATASTORE, 'container_name' : QUALYTICS_SILVER_CONTAINER, 'api_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
)


lc_merge_silver_to_gold = SnowflakeOperator(
    task_id='lc_merge_silver_to_gold',
    dag=dag,
    sql=LOAD_GOLD_TABLE_CMD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

lc_qscan_gold = PythonOperator(
    task_id='lc_qscan_gold',
    dag=dag,
    python_callable=run_scan,
    op_kwargs={'datastore_name' : QUALYTICS_GOLD_DATASTORE, 'container_name' : QUALYTICS_GOLD_CONTAINER, 'api_url': QUALYTICS_API_BASE_URL, 'auth_header' : QUALYTICS_AUTH_HEADER },
)

lc_qscan_raw >> lc_check_scan_raw >> lc_append_raw_to_bronze >> lc_qscan_bronze >> lc_merge_bronze_to_silver >> lc_merge_bronze_to_silver_remediated >> lc_qscan_silver >> lc_merge_silver_to_gold >> lc_qscan_gold

