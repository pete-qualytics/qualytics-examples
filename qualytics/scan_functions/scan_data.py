import time
import requests
import json

def get_datastore_details(datastore_name, api_url, auth_header):
    print(api_url)
    response = requests.get(api_url + 'data-stores', params={'name': datastore_name}, headers=auth_header)
    if response.status_code == 200:
        return response.json()['items'][0]['id']
    elif response.status_code in [403, 401]:
        print('Authentication error')
        return None
    else:
        print('Invalid datastore')
        return None

def run_scan(datastore_name, container_name, api_url, auth_header):
    print(datastore_name)
    ds_id = get_datastore_details(datastore_name, api_url, auth_header)
    if ds_id:
        container_field = ''
        if(container_name.upper() != "ALL"):
            container_field = '"container_names" : ["' + container_name + '"], '
        
        body = '{"type": "scan", ' + container_field + '"data_store_id":' + str(ds_id) + ', ' + '"incremental": "True"}'
        print(body)
        response = requests.post(api_url + 'operations/run', json=json.loads(body), headers=auth_header)
        if response.status_code == 200:
            operation_id = response.json()['id']
            scan_complete = None
            while not scan_complete:
                response = requests.get(api_url + f'operations/{operation_id}', headers=auth_header)
                if response.json()['end_time']:
                    scan_complete = True
                else:
                    time.sleep(10)
            return ds_id
        elif response.status_code in [403, 401]:
            print('Authentication error')
            return None
        else:
            print('failed to initialize operation')
            return None