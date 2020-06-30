import argparse
import sys
import warnings
import re
from functools import reduce

from p_acquisition import m_acquisition as m_ac
from p_wrangling import m_wrangling as m_wr
from p_analysis import m_analysis as m_an

def argument_parser():
    """
    Allows for inputs when running main_script.py in terminal
    """
    parser = argparse.ArgumentParser(description = 'Specify input DB file and API key...')

    # Defined arguments
    parser.add_argument('-p', '--path', type = str, help = 'specify the raw data path...')
    parser.add_argument('-k', '--api_key', type = str, help= 'specify an Quandl API Key to identify user...')

    # Arguments parser
    args = parser.parse_args()
    return args

#-------------------------------------------------------------------------------- acquiring processed data

def acquire_career_info(df_career_info):
    """Career info is list_of_dfs[0]"""
    try:
        print(f'\n\n· Cleaning df_career_info ....')
        # Binarizing data
        df_career_info['dem_full_time_job'] = m_ac.yes_no_to_bool(df_career_info['dem_full_time_job'])

        # Changing nulls by unknown (qualitative params [low, medium, high, unknown])
        df_career_info['dem_education_level'] = m_ac.null_to_unknown(df_career_info['dem_education_level'])

        # Separate cols for boolean options
        initial_cols = ['dem_education_level']
        final_cols = ['High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed', 'Unknown_Ed']

        new_bool_df = m_ac.separate_df_to_bools(df_career_info, initial_cols, final_cols)
        df_career_info = df_career_info.join(other=new_bool_df, on=None, how='left', sort=False)

        # Dropping duplicate cols
        cols_to_del = ['dem_education_level']
        df_career_info.drop(columns=cols_to_del, inplace=True)

        # Save table into local folder
        m_ac.save_df_to_csv(df_career_info,
                       path='data/processed',    # Function adds hierarchy of files
                       name=f'career_info')      # Name of csv

    except:
        print('Something went wrong with [acquire_table_career_info]')

    finally:
        # Memory Usage and objects manually from Jupyter file
        print('''\n\t\t\t  >> Done cleaning df_career_info!. 
        \t\t\t | MEMORY USAGE \t|\t 301.7+ KB -->\t 207.4+ KB
        \t\t\t | DATA \t\t\t|\t objects(4) -->\t bool(6), object(2)
        \t\t\t\t  >> Chekout /data/processed/''')

def acquire_country_info(df_country_info):
    """Country info is list_of_dfs[1]"""
    try:
        print(f'\n\n· Cleaning df_country_info ....')
        # String Operations multiple inputs to binomial cols -> only 2 values from 2 options
        df_country_info['rural'] = m_ac.context_homogenization(df_country_info['rural'])

        # Separate cols for boolean options
        initial_cols = ['rural']
        final_cols = ['rural_context', 'urban_context']

        new_bool_df = m_ac.separate_df_to_bools(df_country_info, initial_cols, final_cols)
        df_country_info = df_country_info.join(other=new_bool_df,
                                               on=None,
                                               how='left',
                                               sort=False)

        df_country_info.drop(columns='rural', inplace=True)

        # Save table into local folder
        m_ac.save_df_to_csv(df_country_info,
                       path='data/processed',    # Function adds hierarchy of files
                       name=f'country_info')     # Name of csv

    except:
        print('Something went wrong with [acquire_career_info]')

    finally:
        print('''\n\t\t\t  >> Done cleaning df_country_info!.
        \t\t\t | MEMORY USAGE \t|\t FROM 337.0+ KB -->\t 226.3+ KB
        \t\t\t | DATA \t\t\t|\t FROM objects(5) -->\t object(3)
        \t\t\t\t  >> Chekout /data/processed/''')

def acquire_personal_info(df_personal_info):
    """Personal info is list_of-dfs[2]"""

    try:
        print(f'\n\n· Cleaning df_personal_info ....')
        # Number normalization
        df_personal_info['age'] = m_ac.ageStr_to_ageNum(serie= df_personal_info['age'])
        df_personal_info['age'] = m_ac.year_to_age(df_personal_info['age'])
        # df_personal_info['age'] = m_ac.year_update(df_personal_info['age']) NOT DONE

        # String Operations: multiple inputs in binomial cols -> only 2 values for 2 options
        df_personal_info['gender'] = m_ac.gender_homogenization(df_personal_info['gender'])
        df_personal_info['dem_has_children'] = m_ac.yes_no_to_bool(df_personal_info['dem_has_children'])

        # Separate cols for boolean options
        initial_cols = ['gender', 'age_group']
        final_cols = ['gender_Female', 'gender_Male', 'ageGroup_14_25', 'ageGroup_26_39', 'ageGroup_40_65',
                      'ageGroup_juvenile']

        new_bool_df = m_ac.separate_df_to_bools(df_personal_info, initial_cols, final_cols)
        df_personal_info = df_personal_info.join(other=new_bool_df, on=None, how='left', sort=False)

        # Save table into local folder
        m_ac.save_df_to_csv(df_personal_info,
                       path='data/processed',    # Function adds hierarchy of files
                       name=f'personal_info')    # Name of csv

    except:
        print('Something went wrong with [acquire_table_personal_info]') # Make a log file

    finally:
        print('''\n\t\t\t  >> Done cleaning df_personal_info!. 
        \t\t\t | MEMORY USAGE \t|\t FROM 337.0+ KB -->\t 367.6+ KB
        \t\t\t | DATA \t\t\t|\t FROM objects(5) -->\t bool(7), object(4)
        \t\t\t\t  >> Chekout /data/processed/''')

def acquire_poll_info(df_poll_info):
    """Poll info is list_of-dfs[3]"""

    sep = ' | '  # this could change
    series = [
        df_poll_info['question_bbi_2016wave4_basicincome_awareness'],
        df_poll_info['question_bbi_2016wave4_basicincome_effect'],
        df_poll_info['question_bbi_2016wave4_basicincome_vote'],
        df_poll_info['question_bbi_2016wave4_basicincome_argumentsagainst'],
        df_poll_info['question_bbi_2016wave4_basicincome_argumentsfor']
        ]

    try:
        print(f'\n\n· Cleaning df_poll_info ....')
        # Deleting strange characters in column ------> this should be as a def in m_ac
        df_poll_info['question_bbi_2016wave4_basicincome_effect'] = [m_ac.split_str_at_char(response, 'Û_ ')
                                                                     if re.search('Û_ ', response)
                                                                     else response
                                                                     for response in df_poll_info[
                                                                         'question_bbi_2016wave4_basicincome_effect']
                                                                     ]

        # Separating AWARENESS POLL -----------> me he quedado aquí
        print(f'..Separating polls')
        df_poll_basicincome_awareness = m_ac.multiple_choice_col_to_df(serie= series[0], separator= sep)
        m_ac.save_df_to_csv(df_poll_basicincome_awareness,
                       path='data/processed',                   # Function adds hierarchy of files
                       name=f'poll_basicincome_awareness')      # Name of csv

        df_poll_basicincome_effect = m_ac.multiple_choice_col_to_df(serie= series[1], separator= sep)
        df_poll_basicincome_vote = m_ac.multiple_choice_col_to_df(serie= series[2], separator= sep)
        df_poll_basicincome_argumentsagainst = m_ac.multiple_choice_col_to_df(serie= series[3], separator= sep)
        df_poll_basicincome_argumentsfor = m_ac.multiple_choice_col_to_df(serie= series[5], separator= sep)

        print(f'..Beginning saving separated polls')
        # Save tables into local folder


        m_ac.save_df_to_csv(df_poll_basicincome_effect,
                       path='data/processed',                   # Function adds hierarchy of files
                       name=f'poll_basicincome_effect')         # Name of csv

        m_ac.save_df_to_csv(df_poll_basicincome_vote,
                       path='data/processed',                   # Function adds hierarchy of files
                       name=f'poll_basicincome_vote')           # Name of csv

        m_ac.save_df_to_csv(df_poll_basicincome_argumentsagainst,
                       path='data/processed',                     # Function adds hierarchy of files
                       name=f'poll_basicincome_argumentsagainst') # Name of csv

        m_ac.save_df_to_csv(df_poll_basicincome_argumentsfor,
                       path='data/processed',                   # Function adds hierarchy of files
                       name=f'poll_basicincome_argumentsfor')   # Name of csv

    except:
        print('Something went wrong with [acquire_table_personal_info]') # Make a log file

    finally:
        print('''\n\t\t\t  >> Done cleaning df_poll_info!. 
                 \t\t\t  >> Chekout /data/processed/''')

#-------------------------------------------------------------------------------- main

def main(some_args):
    print('Beginning pipeline...')
    print(f'\t\t -> arg_path parsed: {arguments.path}')
    print(f'\t\t -> arg_key parsed : {arguments.api_key}')
    print('done parsing arguments!')

    # Now i should iterate through tables, clean them aaand save
    table_names = m_ac.ddbb_tables(m_ac.connect_to_table(arguments.path))
    list_of_dfs = m_ac.fetch_all_from_tables(engine= m_ac.connect_to_table(arguments.path),
                                     table_names_from_ddbb= table_names)  #['career_info', 'country_info', 'personal_info', 'poll_info'] as dfs
    return list_of_dfs


if __name__ == '__main__':
    """
    # Information ddbb
    year_ddbb = '2016'
    year_change = int(input('Enter the year: '))
    title = 'DATA JOBS BY GENDER ' + year_ddbb
    print(main(arguments)[0])  --> esto devuelve career_info, nos entendemos??
    """
    arguments = argument_parser()
    #------------------------------------------------------------------- step 1
    # Getting DataFrames from connection
    list_of_dfs_from_ddbb = main(arguments)   # ['career_info', 'country_info', 'personal_info', 'poll_info']

    # Processing raw_data to analize and saving in local /data/processed
    df_career_info = acquire_career_info(list_of_dfs_from_ddbb[0])
    df_country_info = acquire_country_info(list_of_dfs_from_ddbb[1])
    df_personal_info = acquire_personal_info(list_of_dfs_from_ddbb[2])
    # df_poll_info =acquire_poll_info(list_of_dfs_from_ddbb[3]) something chrashes dont care by now

    #------------------------------------------------------------------- step 2
    m_wr.add_country_col_to_csv()
    m_wr.add_jobs_column_to_csv()



