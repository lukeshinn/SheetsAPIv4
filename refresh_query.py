# Redash Dependencies
import os
import requests
import time
import json
import pandas as pd
from pprint import pprint
from io import StringIO
from io import BytesIO
from typing import Dict, Optional, List

# Sheets Dependencies
import httplib2
from apiclient import discovery
from google.oauth2 import service_account
import numpy as np

def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']

    return None


def get_fresh_query_result(redash_url, query_id, api_key, params: Dict) -> pd.DataFrame:
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    response = s.post('{}/api/queries/{}/refresh'.format(redash_url, query_id), params=params)

    if response.status_code != 200:
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    data = response.json()['query_result']['data']['rows']
    print(data)
    # print(response.content)
    # pd.read_json(json.dumps(data)).to_csv('pd.csv', sep=",")

    return pd.DataFrame(data)

    # return _parse_request(response.content)

def _parse_request(content: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(content))

def sheets_controller(formatted_data):
    try:
        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
        secret_file = os.path.join(os.getcwd(), 'client_secret.json')

        spreadsheet_id = '1b_BL1gQIjfQaQXYu43glKp54FeuTKoahSr1hgLsXhaQ'
        range_name = 'Sheet1!A1:N150'

        credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
        service = discovery.build('sheets', 'v4', credentials=credentials)

        values = [
            ['a1', 'b1', 'c1', 123],
            ['a2', 'b2', 'c2', 456],
        ]

        data = {
            'values' :formatted_data.values.tolist()
        }

        formatted_data.to_csv('formatted.csv')
        # print(formatted_data.values.tolist())

        # TODO: Uncomment when ready to paste to GSheets
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

    except OSError as e:
        print(e)


if __name__ == '__main__':
    params = {'p_param': 1243}
    query_id = 33618
    # Need to use a *user API key* here (and not a query API key).
    api_key = 'og3FzgRfzh7hpQMdsbaAcTsLOxy0ITtcUojXiSVL'
    get_fresh_query_result('https://fulla.thorhudl.com', query_id, api_key, params)
    sheets_controller(get_fresh_query_result('https://fulla.thorhudl.com', query_id, api_key, params))
