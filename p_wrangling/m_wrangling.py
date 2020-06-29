import warnings
import os
import numpy as np
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

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
    # CAMBIAR SEGÚN DICCIONARIO DE EUROPEAN COUNTRIES
    """
    INPUT  ->      AT      FR     ES  -> alpha_2 code
    OUTPUT -> Austria  France  Spain  -> full name
    --------------------------------------------------------------------------------
    Note: countries_code_alpha_2 is a dict from get_dictCountries()
    """
    print('\t ··· Fetching European countries from web scrapping')
    country_dictionary = get_dictEuropeanCountries()

    return serie.apply(lambda x: country_dictionary[str(x)])  # Esto se puede hacer en la API

def add_country_col_to_csv():
    """
    Opens csv, transforms it to a DF and adds a Col with WEB Information
    """
    # Variables
    path = 'data/processed/'
    name = 'country_info'
    col_name_to_add = 'country_names'
    col_name_reference = 'country_code'

    print(f'\n\n· Adding column to csv located at {path}....')
    try:
        print(f' ·· Opening {name}.csv ....')

        # Action to apply
        path_df = str(path) + str(name) + '.csv'
        print(path_df)
        df_to_change = pd.read_csv(path_df,
                                   index_col=[0])
        print(f'\t ··· df_to_change..')
        df_to_change[col_name_to_add] = countryCode_to_countryName(serie=df_to_change[col_name_reference])

        # Save table into local folder
        print(f'\t ··· ready to rewrite..')
        m_ac.save_df_to_csv(df_to_change,
                            path=path,  # Function adds hierarchy of files
                            name=name)  # Name of csv

    except:
        print('Something went wrong at [add_col_to_csv]')

    finally:
        print('''\t\t\t  >> Done adding web scrapping information!.
        \t\t\t\t   >> Chekout /data/processed/''')

#-------------------------------------------------------------------------------- API Connection Exercise

def get_job_code(serie):
    """
    INPUT  -> hash
    OUTPUT -> dictionary w hash info provided by API
    """
    json_dicts = []
    uuid_db = serie.unique().tolist()

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
    new_col = [d.get('title') for job_code in coded_series
               for d in json_data
               if d.get('uuid') == job_code]
    return new_col  # Con apply or merge tb debería funcionar

def add_jobs_column_to_csv():
    """
    Opens csv, transforms it to a DF and adds a Col with WEB Information
    """
    # Variables
    path = 'data/processed/'
    name = 'career_info'
    col_name_to_add = 'job_names'
    col_name_reference = 'normalized_job_code'

    print(f'\n\n· Adding column to csv located at {path}....')
    try:
        print(f' ·· Opening {name}.csv ....')

        # Action to apply
        path_df = str(path) + str(name) + '.csv'
        print(path_df)

        df_to_change = pd.read_csv(path_df, index_col=[0])
        json_job_data = get_job_code(df_to_change[col_name_reference])

        print(f'\t ··· df_to_change..')
        df_to_change[col_name_to_add] = normalized_jobs_col(coded_series=df_to_change[col_name_reference],
                                                            json_data=json_job_data)
        # Save table into local folder
        print(f'\t ··· ready to rewrite..')
        m_ac.save_df_to_csv(df_to_change,
                            path=path,  # Function adds hierarchy of files
                            name=name)  # Name of csv

    except:
        print('Something went wrong at [add_col_to_csv]')

    finally:
        print('''\t\t\t  >> Done adding web scrapping information!.
        \t\t\t\t   >> Chekout /data/processed/''')
