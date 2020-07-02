import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from p_acquisition import m_acquisition as m_ac

#-------------------------------------------------------------------------------- WEB Scrapping Exercise

def get_dictEuropeanCountries():
    wiki_url = 'https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2'
    url = 'https://www.euro.who.int/en/countries'

    html_wikipage = requests.get(wiki_url).content
    html_eurocountries = requests.get(url).content

    countries_list = pd.read_html(html_wikipage, header=0)[2]
    countries_dict = dict(zip(countries_list['Code'], countries_list['Country name (using title case)']))

    soup = BeautifulSoup(html_eurocountries, 'lxml')
    table = soup.find_all('section', {'class': 'clearfix'})

    all_contries = [content.text for content in table]
    eu_countries = list(filter(None, all_contries[0].split('\n')))

    european_countries_values = [val for k, val in countries_dict.items() for eu_c in eu_countries if val == eu_c]
    european_countries_key = [k for k, val in countries_dict.items() for eu_c in eu_countries if val == eu_c]

    return dict(zip(european_countries_key, european_countries_values))

def countryCode_to_countryName(serie):
    """
    INPUT   ->       AT      FR     ES   ->  alpha_2 code
    OUTPUT  ->  Austria  France  Spain   ->  full name in dict form
    """
    print('\t ··· Fetching European countries from web scrapping')
    country_dictionary = get_dictEuropeanCountries()

    return serie.apply(lambda x: country_dictionary[str(x)])

def add_country_col(df_to_change):
    """
    Transforms DF and adds a Col with WEB Information, saves it and returns it
    """
    # Variables
    path = 'data/processed/'
    name = 'country_info'
    col_name_to_add = 'country_names'
    col_name_reference = 'country_code'

    print(f'\n\n· Adding column to csv located at {path}....')
    try:
        # Action to apply
        df_to_change[col_name_to_add] = countryCode_to_countryName(serie=df_to_change[col_name_reference])

        # Save table into local folder
        print(f'\t ··· ready to rewrite..')
        m_ac.save_df_to_csv(df_to_change,
                            path=path,  # Function adds hierarchy of files
                            name=name)  # Name of csv
        return df_to_change
    except:
        print('Something went wrong at [add_col_to_csv]')

    finally:
        print('''\t\t\t  >> Done adding web scrapping information!.
        \t\t\t\t   >> Chekout /data/processed/''')

#-------------------------------------------------------------------------------- API Connection Exercise

def download_file_from_API(full_url, list_of_dicts):
    """
    INPUT  -> list to dump dict from URL
    OUTPUT -> list full of web response
    """
    try:
        response = requests.get(full_url, stream=True)
        list_of_dicts.append(response.json())
        return list_of_dicts

    except requests.exceptions.RequestException as e:
        return e

def threads_runner_for_API(uuid_db):
    """
    INPUT  -> given a serie, send simultanious requests to API
    OUTPUT -> list_of_dicts with all dicts from API requests
    """
    # Variables
    threads = []
    list_of_dicts = []
    API_url_path = 'http://api.dataatwork.org/v1/jobs/'

    with ThreadPoolExecutor(max_workers=16) as executor:
        # max_workers between 10 and 20 works fine.. depends on available CPU threads
        for job_code in uuid_db:
            full_url = f'{API_url_path}{job_code}'
            threads.append(executor.submit( download_file_from_API, full_url, list_of_dicts))

    for task in as_completed(threads):
        return list_of_dicts

def get_normalized_jobs_col(coded_series, json_data):
    """
    d.get('title') or d.get('normalized_job_title') they are pretty much the same
    -----------------------------------------------------------------------------
    INPUT  -> None  <hash>                 --> coded_col
    OUTPUT -> None  <job name as in API>   --> new_col
    """
    new_col = [d.get('title') for job_code in coded_series
               for d in json_data
               if d.get('uuid') == job_code]
    return new_col

def change_temp_df(list_of_dict_API, df_to_change, col_name_reference, col_name_to_add):
    columns = ['uuid', 'title']

    dict_df = pd.DataFrame.from_records(list_of_dict_API, columns=columns)
    dict_df.rename(columns={columns[0]: col_name_reference, columns[1]:col_name_to_add},
                   inplace=True)
    df_changed = df_to_change.merge(dict_df, how='left',
                                             on=col_name_reference)

    return df_changed

def add_jobs_column(df_to_change):
    """
    Opens csv, transforms it to a DF and adds a Col with WEB Information
    """
    # Variables
    path = 'data/processed/'
    csv_name = 'career_info'
    col_name_to_add = 'normalized_job_names'
    col_name_reference = 'normalized_job_code'

    print(f'\n\n· Adding column to csv located at {path}....')
    try:
        uuid_db = df_to_change[col_name_reference].unique().tolist()

        print(f' ·· Threading API response to get dicts...')
        lst_dicts = threads_runner_for_API(uuid_db)
        df_changed = change_temp_df(list_of_dict_API= lst_dicts,
                                    df_to_change=df_to_change,
                                    col_name_reference=col_name_reference,
                                    col_name_to_add= col_name_to_add)

        print(f' ·· Adding new col to {csv_name}.csv ....')
        m_ac.save_df_to_csv(df_changed,
                            path=path,      # Function adds hierarchy of files
                            name=csv_name)  # Name of csv"""
        return df_changed
    except:
        print('Something went wrong at [add_col_to_csv]')

    finally:
        print('''\t\t\t  >> Done adding web scrapping information!.
            \t\t\t\t   >> Checkout /data/processed/''')
