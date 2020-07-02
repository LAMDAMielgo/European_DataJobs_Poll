import argparse
import pandas as pd
import time
#------------------------------------------------------------------- import packages
from p_acquisition import m_acquisition as m_ac
from p_acquisition import m_cleaning as m_cl
from p_wrangling import m_wrangling as m_wr
from p_analysis import m_analysis as m_an
from p_reporting import m_reporting as m_rep
#------------------------------------------------------------------- display pandas option
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 500)
#------------------------------------------------------------------- main functions
def argument_parser():
    parser = argparse.ArgumentParser(description = 'Specify input DB file and API key...')
    parser.add_argument('-c', '--country', type= str, nargs='*', help= 'specify an European Country to choose from...', required=False)
    args = parser.parse_args()
    return args

def main(some_args):
    print(f'\t\t -> arg_path parsed: {arguments.country}')
    print('done parsing arguments!')

    country = arguments.country
    return country

def ddbb_tables(ddbb_path='data/raw/raw_data_project_m1.db'):
    """ddbb_path = 'data/raw/raw_data_project_m1.db'"""

    table_names = m_ac.ddbb_tables(m_ac.connect_to_table(ddbb_path))
    list_of_dfs = m_ac.fetch_all_from_tables(engine=m_ac.connect_to_table(ddbb_path),
                                             table_names_from_ddbb=table_names)
    print(f'''Created connection with DDBB
                  Table names -> \t{table_names}''')
    return list_of_dfs

#-------------------------------------------------------------------

if __name__ == '__main__':
    # Global
    NUM_TOP_SKILLS = 5

    start_titles = '''
    <================= PIPELINE PROJECT =================>
    <=== JOBS IN DATA BY [GENDER] + POLL [BASICINCOME] ==>
    <================ SAMPLE YEAR = 2016 ================>
    '''
    arguments = argument_parser()
    country_argument = arguments.country

    ## aquí al principio poner country y avisar si country correcto o no
    list_of_dfs_from_ddbb = ddbb_tables()  # ['career_info', 'country_info', 'personal_info', 'poll_info']
    #------------------------------------------------------------------- processing raw_data to analize and saving in local
    df_career_info = m_wr.add_jobs_column(m_cl.acquire_career_info(list_of_dfs_from_ddbb[0]))
    df_country_info = m_wr.add_country_col(m_cl.acquire_country_info(list_of_dfs_from_ddbb[1]))
    df_personal_info = m_cl.acquire_personal_info(list_of_dfs_from_ddbb[2])
    df_poll_info = m_cl.acquire_poll_info(list_of_dfs_from_ddbb[3])
    # ------------------------------------------------------------------- checking for country arg_parse validation or exit
    # ------------------------------------------------------------------- getting final tables
    list_of_df_cleaned = [df_career_info, df_country_info, df_personal_info]
    df_job_gender = m_an.get_percentages_gender_by_job(m_an.get_base_analysis_df(country_argument, list_of_df_cleaned))
    df_top_skills = m_an.get_df_top_skills(country_argument, NUM_TOP_SKILLS, list_of_df_cleaned)
    # ------------------------------------------------------------------- getting final visualizations
    male_top_skills = m_rep.get_stacked_bar_plot(m_an.get_df_top_skills(country_argument, NUM_TOP_SKILLS, list_of_df_cleaned, gender='M'))
    female_top_skills = m_rep.get_stacked_bar_plot(m_an.get_df_top_skills(country_argument, NUM_TOP_SKILLS, list_of_df_cleaned, gender='F'))

    print('<==================== [[ END ]] =====================>')