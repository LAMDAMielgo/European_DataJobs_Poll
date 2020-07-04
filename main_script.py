import argparse
import time
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
#------------------------------------------------------------------- import packages
from p_acquisition import m_acquisition as m_ac
from p_acquisition import m_cleaning as m_cl
from p_wrangling import m_wrangling as m_wr
from p_analysis import m_analysis as m_an
from p_reporting import m_reporting as m_rep
#------------------------------------------------------------------- global
NUM_TOP_SKILLS = 5
#------------------------------------------------------------------- main functions
def argument_parser():
    parser = argparse.ArgumentParser(description = 'Specify input DB file and API key...')
    parser.add_argument('-c', '--country', type= str, nargs='*', help= 'specify an European Country to choose from...', required=False)
    args = parser.parse_args()
    return args

def main(some_args):
    print(f' · Parsing argument: {arguments.country}')

    country = m_ac.list_to_string(arguments.country)
    print('\t ··· Fetching European countries from web scrapping')
    european_countries = m_wr.get_dictEuropeanCountries()

    print(f'\t ··· Validating country argparse')
    if m_an.country_argparse_eval(country, list(european_countries.values())) == country:
        print(f' ·· country_argument found in ddbb')
        print(f'\t\t\t\t\t >> continuing w script...')
        country_validated = country
        return country_validated, european_countries
    elif country == "All" or country == "":
        print(f'\t\t >> getting all countries')
        country_validated = ''
        return country_validated, european_countries
    else:
        print(f'\t\t >> country_argument not found.')
        print(f'\t\t >> proceeding to exit')
        time.sleep(3)
        exit()

#-------------------------------------------------------------------
if __name__ == '__main__':
    print(f'''
    <================= PIPELINE PROJECT =================>
    <=== JOBS IN DATA BY [GENDER] + POLL [BASICINCOME] ==>
    <================ SAMPLE YEAR = 2016 ================>\n''')
    arguments = argument_parser()
    print(f'\n[1] VALIDATING ARGUMENT =============>')
    country_argument, dict_european_countries = main(arguments.country)
    print(f'\n[2] ACQUIRING RAW DATA ==============>')
    list_of_raw_dfs_from_bbdd = m_ac.get_ddbb()  # ['career_info', 'country_info', 'personal_info', 'poll_info']
    print(f'\n[3] CLEANING AND SAVING IN LOCAL ====>')
    list_of_df_info_cleaned = m_cl.get_all_info_tables(list_of_raw_dfs_from_bbdd[0:-1], dict_european_countries) # ['career_info', 'country_info', 'personal_info']
    list_of_df_polls_cleaned = m_cl.acquire_poll_info(list_of_raw_dfs_from_bbdd[-1])    # [[df_1,..., df_5], [col_name_1,...,col_name_5]]
    print(f'\n[4] DATA ANALYSIS ====>')
    df_job_gender = m_an.get_percentages_gender_by_job(m_an.get_base_analysis_df(country_argument, list_of_df_info_cleaned))  # table from challenge 1
    df_top_skills = m_an.get_df_top_skills(country_argument, NUM_TOP_SKILLS, list_of_df_info_cleaned) # [[ top_skills_bonus_3], [counts_bonus_3]]
    print(f'\n[5] DATA VISUALIZATION ====>')
    m_rep.distribution_top_skills(country_argument, NUM_TOP_SKILLS, list_of_df_info_cleaned, genders= ['F', 'M'])
    m_rep.distribution_BI_arguments(country_argument, list_of_df_info_cleaned, list_of_df_polls_cleaned)
    print('<==================== [[ END ]] =====================>')



