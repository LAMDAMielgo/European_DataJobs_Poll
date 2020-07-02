import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

def creating_empty_dict(path, file_name):
    completed_path = str(path) + str(file_name) + '.json'
    with open(f'{completed_path}', 'w+') as fp:
        pass
    return (f'\t ··· New file created')

def download_file_from_API(path, file_name, url):
    completed_path = str(path) + str(file_name) + '.json'
    try:
        response = requests.get(url, stream=True)
        with open(f'{completed_path}', 'a+') as f:
            json.dump(response.json(), f, indent=4)

    except requests.exceptions.RequestException as error:
        return error

def threads_runner_for_API(path, file_name):
    threads = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        for job_code in uuid_db:
            full_url = f'http://api.dataatwork.org/v1/jobs/{job_code}'
            threads.append(executor.submit(download_file_from_API, path, file_name, full_url))

            return(f'\t ··· jobs_dict created')

    for task in as_completed(threads):
        print(task.result())

# Variables
path = 'data/processed/'
csv_name = 'career_info'
file_name = 'jobs_dict'
col_name_to_add = 'job_names'
col_name_reference = 'normalized_job_code'


path_df = str(path) + str(csv_name) + '.csv'
df_to_change = pd.read_csv(path_df, index_col=[0])
uuid_db = df_to_change[col_name_reference].unique().tolist()

creating_empty_dict(path, file_name)
print('we are here')
threads_runner_for_API(path, file_name)

json_file = json.loads('jobs_dict.json')
print(len(json_file))