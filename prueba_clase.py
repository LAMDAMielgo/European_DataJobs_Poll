import pandas as pd
import requests
import json
import threading
import multiprocessing

# Variables
path = 'data/processed/'
name = 'career_info'
col_name_to_add = 'normalized_job_name'
col_name_reference = 'normalized_job_code'


path_df = str(path) + str(name) + '.csv'
df_to_change = pd.read_csv(path_df, index_col=[0])

uuid_db = df_to_change[col_name_reference].unique().tolist()
url = f'http://api.dataatwork.org/v1/jobs/'

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed


def download_file(url, file_name):
    try:
        response = requests.get(url, stream=True)
        # json_data = response.json()

        with open(f'{file_name}.json', 'a+') as f:
            json.dump(response.json(), f, indent=4)

        return print('done')

    except requests.exceptions.RequestException as e:
        return e

def runner():
    threads = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        for job_code in uuid_db:
            full_url = f'http://api.dataatwork.org/v1/jobs/{job_code}'
            file_name = 'prueba'

            threads.append(executor.submit(download_file, full_url, file_name))
    for task in as_completed(threads):
        print(task.result())


runner()

json_file = json.loads('prueba.json')
print(len(json_file))