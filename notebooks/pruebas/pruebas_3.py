import pandas as pd
import requests


from p_acquisition import m_acquisition as m_ac

def get_job_code(serie_reference):
    """
    In table career_info
    INPUT  -> hash
    OUTPUT -> dictionary w hash info provided by API
    """
    json_dicts = []
    uuid_db = serie_reference.unique().tolist()

    for job_code in uuid_db:
        response = requests.get(f'http://api.dataatwork.org/v1/jobs/{job_code}')
        json_dicts.append(response.json())

    return json_dicts

def normalized_jobs_col(coded_series, json_data):
    """
    d.get('title') or d.get('normalized_job_title') they are pretty much the same
    -----------------------------------------------------------------------------
    INPUT  -> None  <hash>                 --> coded_col
    OUTPUT -> None  <job name as in API>   --> new_col
    """
    print(f'\t\t ··· Fetching col')

    new_col = []
    for job_code in coded_series:
        for d in json_data:
            if d.get('uuid') == job_code:
                new_col.append(d.get('title') )
            else:
                new_col.append(None)

    print(f'\t\t ··· Done col')
    return new_col

# Variables
path = '../../data/processed/'
csv_name = 'career_info'
file_name = 'jobs_dict'
col_name_to_add = 'normalized_job_names'
col_name_reference = 'normalized_job_code'

print(f'\n\n· Adding column to csv located at {path}....')

# Action to apply
path_df = str(path) + str(csv_name) + '.csv'

df_to_change = pd.read_csv(path_df, index_col=[0])

serie_reference = df_to_change[col_name_reference]

print(f' ·· Fetching data from API. This may take some time [2min] ....')
json_job_data = get_job_code(serie_reference)

print(df_to_change.shape)
print(len(json_job_data))
print(f' ·· Adding new col to {csv_name}.csv ....')
df_to_change[col_name_to_add] = normalized_jobs_col(coded_series=serie_reference,
                                                    json_data=json_job_data)

# Save table into local folder
print(f'\t ··· ready to rewrite..')
m_ac.save_df_to_csv(df_to_change,
                    path=path,  # Function adds hierarchy of files
                    name=name)  # Name of csv

