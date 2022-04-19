"""
this prefect demo integrates with Qualytics and allows API calls to get scan results
"""
#from prefect import Flow, Parameter
#from plot_functions.plot_data import analyze_scan
from pyparsing import empty
from auth.get_token import get_token
from load_functions.load_data import scan
from datetime import timedelta, datetime
#from prefect.schedules import IntervalSchedule


# optional scheduling of operations
#schedule = IntervalSchedule(
#    start_date=datetime.utcnow() + timedelta(seconds=1),
#    interval=timedelta(hours=24),
#)

#with Flow("read-s3", schedule=schedule) as flow:
    # define parameters for scan
#datastore_name = Parameter('ds_name', default='lending-club-lakehouse')
#api_base_url = Parameter('api_url', default='https://databricks.qualytics.io/api/')
datastore_name = 'lending-club-raw'
api_base_url = 'https://databricks.qualytics.io/api/'

auth_header = get_token()
print(auth_header)
# scan a JDBC store on schedule
container_name = 'ALL'
scan(datastore_name=datastore_name, container_name=container_name, api_base_url=api_base_url, auth_header=auth_header)

datastore_name = 'lending-club-lakehouse'
container_name = 'SILVER_LC_LOANS'
scan(datastore_name=datastore_name, container_name=container_name, api_base_url=api_base_url, auth_header=auth_header)

#if __name__ == '__main__':
#    flow.visualize()
#    flow_state = flow.run()
#    flow.visualize(flow_state=flow_state)
