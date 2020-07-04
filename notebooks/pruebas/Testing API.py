import warnings
import os

import numpy as np
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

from functools import reduce


##################################################################  FUNCTIONS
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

################################################################

european_countries = get_dictEuropeanCountries()

def chosing_db():
    while True:

        print(f'''
            PROCESS DATA: DISTRIBUTION BY GENDER
            Select type of filtering

            [0] Select an european country
            [1] See all countries              
            ''')
        country_option = input("Choose an option [0|1]....")

        if country_option == '0':
            while True:
                user_country = input("Which country do you want to see?....")
                if user_country in european_countries.values():
                    print(f'\t\t ->{user_country} found in DataBase!')
                    return user_country
                    exit(os.EX_OK)
                    ## Continue with table w/ country
                else:
                    print(f'\t\t ->{user_country} missing in DataBase. PLease try again')
                    ## Continue with table w/ all countries

        elif country_option == '1':
            print(f'Show all countries [table 1]')
            return european_countries
            print()
            print(european_countries)
            pass


user_country = chosing_db()

print(user_country)
