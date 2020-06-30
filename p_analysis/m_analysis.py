
import pandas as pd
from functools import reduce

from p_acquisition import m_acquisition as m_ac

def opening_all_cleaned_data(list_of_tables, index_col, path):
    """
    df_personal_info = pd.read_csv('../data/processed/personal_info.csv', index_col=[0])
    df_career_info = pd.read_csv('../data/processed/career_info.csv', index_col=[0])
    df_country_info = pd.read_csv('../data/processed/country_info.csv', index_col=[0]
    """
    pass

def get_base_analysis_df(country_argument):
    """
    Returns base_df for analysis after evaluating country argument
    """
    dfs = [
        df_country_info[['uuid', 'country_names']],
        df_personal_info[['uuid', 'gender']],
        df_career_info[
            ['uuid', 'normalized_job_names', 'dem_full_time_job', 'High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed']],
    ]

    df_final = reduce(lambda left, right: pd.merge(left, right, on='uuid'), dfs)

    return country_argparse_filter(country_argument.capitalize(), df_final)

#-------------------------------------------------------------------------------- Creating First table (Jobs-Gender)

def country_argparse_eval(country_argument, list_to_search):
    """
    Evaluates whether argument exists in df_country_info[country_names]
    """
    print(f'\t ··· Validating country argparse')

    if country_argument in list_to_search:
        print(f'\t\t >> country_argument found in ddbb')
        def_country_argument = country_argument
        return def_country_argument

    else:
        print(f'\t\t >> country_argument not found')
        def_country_argument = ""
        return def_country_argument

def country_argparse_filter(country_argument, analysis_df):
    """
    Return analysis df by argument input
    """
    list_to_search = analysis_df['country_names'].unique().tolist()
    c_arg = country_argparse_eval(country_argument, list_to_search)

    if c_arg == "":
        return analysis_df
    else:
        return analysis_df[analysis_df['country_names'] == c_arg]

def get_percentages_gender_by_job(base_analysis_df):
    # Variables.
    filtr = ['country_names', 'normalized_job_names', 'gender']

    drop_cols = ['uuid', 'dem_full_time_job',
                 'High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed',
                 'totals_per_country']

    new_cols = ['quantity', 'percentage']

    # Here is missing opening CSV from local

    # Add first col = quality
    df_job_gender = base_analysis_df.assign(quantity=1) \
        .drop(columns=drop_cols[0:2]) \
        .groupby(filtr) \
        .agg('count') \
        .reset_index()

    # Generate totals_per_country
    df_total_per_country = df_job_gender.groupby(filtr[0]) \
        [filtr[1]] \
        .nunique() \
        .to_frame() \
        .rename(columns={filtr[1]: drop_cols[-1]})

    df_job_gender = df_job_gender.merge(df_total_per_country, on=filtr[0])

    # Add second col == percentage and deleting totals_per_country when not need
    df_job_gender[new_cols[1]] = round(df_job_gender[new_cols[0]] / df_job_gender[drop_cols[-1]] * 100, 3)
    df_job_gender.drop(columns=[drop_cols[-1]], inplace=True)

    return df_job_gender

#-------------------------------------------------------------------------------- Bonus 1: Work with Polls



#-------------------------------------------------------------------------------- Bonus 2: Top Skills

def top_skills_by_ed_level(base_analysis_df, number_skills, ed_level):
    # Variables
    filtr = 'normalized_job_names'
    drop_cols = ['uuid', 'country_names', 'gender', 'dem_full_time_job']

    # Filter by education level
    df_high_ed = base_analysis_df[df_analysis[ed_level]] \
        .drop(columns=['High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed'])

    # Counting and sorting
    df_high_ed_skills_gender = df_high_ed.assign(counts=1) \
        .drop(columns=drop_cols) \
        .groupby(filtr) \
        .agg('count') \
        .reset_index()

    df_high_ed_skills_gender.sort_values(by=['counts'], ascending=False, inplace=True)
    # Getting serie
    pd = df_high_ed_skills_gender[filtr].reset_index() \
        .head(number_skills)

    pd = pd.rename(columns={filtr: ed_level}).drop(columns='index')

    return pd


def get_df_top_skills(country_argument, num_top_skills):
    # Variables
    ed_levels = ['High_Ed', 'Medium_Ed', 'Low_Ed']

    # Single column DF
    serie_top_skills_high_ed = top_skills_by_ed_level(base_analysis_df=get_base_analysis_df(
        country_argument=country_argument),
        number_skills=num_top_skills,
        ed_level=ed_levels[0])

    serie_top_skills_medium_ed = top_skills_by_ed_level(base_analysis_df=get_base_analysis_df(
        country_argument=country_argument),
        number_skills=num_top_skills,
        ed_level=ed_levels[1])

    serie_top_skills_low_ed = top_skills_by_ed_level(base_analysis_df=get_base_analysis_df(
        country_argument=country_argument),
        number_skills=num_top_skills,
        ed_level=ed_levels[2])

    # Construccion of DF
    all_dfs = [serie_top_skills_high_ed, serie_top_skills_medium_ed, serie_top_skills_low_ed]
    cols = dict(zip([n for n in range(5)], ['#' + str(n) for n in range(5)]))

    result = pd.concat(all_dfs, axis=1, sort=False)
    return result.T.rename(columns=cols)