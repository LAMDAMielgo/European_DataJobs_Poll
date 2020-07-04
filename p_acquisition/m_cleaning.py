
from p_acquisition import m_acquisition as m_ac
from p_acquisition import m_cleaning as m_cl
from p_wrangling import m_wrangling as m_wr

#-------------------------------------------------------------------------------- global variables
PATH_TO_SAVE_CSV = 'data/processed'
FILE_NAMES_FOR_CSV = ['career_info', 'country_info', 'personal_info', 'poll_basicincome_awareness',
                   'poll_basicincome_vote', 'poll_basicincome_effect', 'poll_basicincome_argumentsfor',
                   'poll_basicincome_argumentsagainst']

SEPARATOR_IN_POLLS = ' | '
#-------------------------------------------------------------------------------- cleaning and saving tables

def acquire_career_info(df):
    """Career info is list_of_dfs[0]"""
    # Variables for this df
    initial_cols = ['dem_education_level']
    final_cols = ['High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed', 'Unknown_Ed']

    try:
        print(f'· Cleaning df_career_info ....')
        # Binarizing data
        df['dem_full_time_job'] = m_ac.yes_no_to_bool(df['dem_full_time_job'])

        # Changing nulls by unknown (qualitative params [low, medium, high, unknown])
        df['dem_education_level'] = m_ac.null_to_unknown(df['dem_education_level'])

        new_bool_df = m_ac.separate_df_to_bools(df, initial_cols, final_cols)
        df = df.join(other=new_bool_df, on=None, how='left', sort=False)

        # Dropping duplicate cols
        cols_to_del = ['dem_education_level']
        df.drop(columns=cols_to_del, inplace=True)

        # Save table into local folder
        m_ac.save_df_to_csv(df, path=PATH_TO_SAVE_CSV, name=FILE_NAMES_FOR_CSV[0])

        return df
    except:
        print('Something went wrong with [acquire_table_career_info]')

    finally:
        # Memory Usage and objects manually from Jupyter file
        print('''\n\t\t\t  >> Done cleaning df_career_info!. 
        \t\t\t\t  >> Chekout /data/processed/\n''')

def acquire_country_info(df):
    """Country info is list_of_dfs[1]"""
    # Variables for this df
    initial_cols = ['rural']
    final_cols = ['rural_context', 'urban_context']

    try:
        print(f'· Cleaning df_country_info ....')
        # String Operations multiple inputs to binomial cols -> only 2 values from 2 options
        df['rural'] = m_ac.context_homogenization(df['rural'])

        new_bool_df = m_ac.separate_df_to_bools(df, initial_cols, final_cols)
        df = df.join(other=new_bool_df, on=None, how='left', sort=False)
        df.drop(columns='rural', inplace=True)

        # Save table into local folder
        m_ac.save_df_to_csv(df, path=PATH_TO_SAVE_CSV, name=FILE_NAMES_FOR_CSV[1])

        return df

    except:
        print('Something went wrong with [acquire_career_info]')

    finally:
        print('''\t\t\t  >> Done cleaning df_country_info!.
        \t\t\t\t  >> Chekout /data/processed/\n''')

def acquire_personal_info(df):
    """Personal info is list_of-dfs[2]"""
    # Variables in df
    initial_cols = ['age_group']
    final_cols = ['ageGroup_14_25', 'ageGroup_26_39', 'ageGroup_40_65', 'ageGroup_juvenile']

    try:
        print(f'· Cleaning df_personal_info ....')
        # Number normalization
        df['age'] = m_ac.ageStr_to_ageNum(serie= df['age'])
        df['age'] = m_ac.year_to_age(df['age'])

        # String Operations: multiple inputs in binomial cols -> only 2 values for 2 options
        df['gender'] = m_ac.gender_homogenization(df['gender'])
        df['dem_has_children'] = m_ac.yes_no_to_bool(df['dem_has_children'])

        # Separate cols for boolean options
        new_bool_df = m_ac.separate_df_to_bools(df, initial_cols, final_cols)
        df = df.join(other=new_bool_df, on=None, how='left', sort=False)

        # Save table into local folder
        m_ac.save_df_to_csv(df, path=PATH_TO_SAVE_CSV, name=FILE_NAMES_FOR_CSV[2])

        return df

    except:
        print('Something went wrong with [acquire_table_personal_info]') # Make a log file

    finally:
        print('''\t\t\t  >> Done cleaning df_personal_info!. 
        \t\t\t\t  >> Chekout /data/processed/\n''')


def get_separate_df(serie_to_eval, separator_string, file_name):
    """
    INPUT   -> inputs to calls formentioned defs
    OUTPUT  -> saves them into a common zip file and return a list of alls dfs
    """
    print(f'\t ···· Iterating through poll lists')
    df = m_ac.multiple_choice_col_to_df( serie= serie_to_eval, separator= separator_string)
    m_ac.save_df_to_csv(df, path=PATH_TO_SAVE_CSV, name=file_name)

    return df

def acquire_poll_info(df):
    """Poll info is list_of-dfs[3]"""
    # Variables for this df
    series_cols = list(df.columns)[1:]
    files_names = FILE_NAMES_FOR_CSV[3:]
    list_of_separated_polls = []

    print(f'· Cleaning df_poll_info ....')
    # Deleting strange characters in column ------> this should be as a def in m_ac
    df[series_cols[3]] = m_ac.get_serie_at_split_str_at_char(ser= df[series_cols[3]], char= 'Û_ ')

    # Creating a list of series as an iterable, getting separate polls_info as iter and saving them into zip
    for column, file_name in zip(series_cols, files_names):
        separated_polls = get_separate_df(serie_to_eval= df[column],
                                          separator_string= SEPARATOR_IN_POLLS,
                                          file_name= file_name)
        list_of_separated_polls.append(separated_polls)

    return [list_of_separated_polls, series_cols]

#-------------------------------------------------------------------------------- GETTING ALL TABLES

def get_all_info_tables(raw_dfs, dict):
    df_career_info = m_wr.add_jobs_column(m_cl.acquire_career_info(raw_dfs[0]))
    df_country_info = m_wr.add_country_col(m_cl.acquire_country_info(raw_dfs[1]), countries_dict =dict)
    df_personal_info = m_cl.acquire_personal_info(raw_dfs[2])

    return [df_career_info, df_country_info, df_personal_info]