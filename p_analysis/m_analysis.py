import pandas as pd
from functools import reduce
from p_acquisition import m_acquisition as m_ac

def get_base_analysis_df(country_argument, list_of_clean_df):
    """
    Returns base_df for analysis after evaluating country argument
    """
    # list_of_clean_df = [df_career_info, df_country_info, df_personal_info] from main
    dfs = [
        list_of_clean_df[1][['uuid', 'country_names']],
        list_of_clean_df[2][['uuid', 'gender']],
        list_of_clean_df[0][
            ['uuid', 'normalized_job_names', 'dem_full_time_job', 'High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed']]
        ]

    df_final = reduce(lambda left, right: pd.merge(left, right, on='uuid'), dfs)
    print(country_argument)
    country = ' '.join([arg for arg in country_argument])
    return country_argparse_filter(country.capitalize(), df_final)

#-------------------------------------------------------------------------------- Evals country_argparse
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

#-------------------------------------------------------------------------------- Creating First table (Jobs-Gender)
def get_percentages_gender_by_job(base_analysis_df):
    """
    This function solves the main
    """

    # Variables.
    filtr = ['country_names', 'normalized_job_names', 'gender']

    drop_cols = ['uuid', 'dem_full_time_job',
                 'High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed',
                 'totals_per_country']

    new_cols = ['quantity', 'percentage']

    # Add first col = quantity
    df_job_gender = base_analysis_df.assign(quantity= 1) \
                                    .drop(columns=drop_cols[0:-1]) \
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

    # Save table into local folder
    m_ac.save_df_to_csv(df_job_gender,
                        path='data/results',  # Function adds hierarchy of files
                        name=f'df_percentage_by_job_and_gender')  # Name of csv
    return df_job_gender

#-------------------------------------------------------------------------------- Bonus 1: Top Skills: Quizá meter esto en REPORTING
def top_skills_by_ed_level(base_analysis_df, number_skills, ed_level, gender_to_eval):
    # Variables
    filtr = 'normalized_job_names'
    drop_cols = ['uuid', 'country_names', 'gender', 'dem_full_time_job']

    # Filter by education level
    df_high_ed = base_analysis_df[base_analysis_df[ed_level]] \
                .drop(columns= ['High_Ed', 'Low_Ed', 'Medium_Ed', 'No_Ed'])

    # Getting only one gender
    if gender_to_eval != "All":
        df_high_ed = df_high_ed[df_high_ed['gender'] == gender_to_eval]
    else:
        df_high_ed = df_high_ed

    # Counting and sorting
    df_high_ed_skills_gender = df_high_ed.assign(counts= 1) \
                                         .drop(columns= drop_cols) \
                                         .groupby(filtr) \
                                         .agg('count') \
                                         .reset_index()

    df_high_ed_skills_gender.sort_values(by=['counts'], ascending=False, inplace=True)
    counts_by_gender = df_high_ed_skills_gender['counts'].tolist()

    pd = df_high_ed_skills_gender[filtr].reset_index().head(number_skills)

    pd = pd.rename(columns={filtr: ed_level}).drop(columns='index')
    counts = counts_by_gender[:number_skills]

    # returns tuple
    return pd, counts

def get_df_top_skills(country_argument, num_top_skills, list_dfs_cleaned, gender="All"):
    # Variables
    ed_levels = ['High_Ed', 'Medium_Ed', 'Low_Ed']
    path_to_save = 'data/results'

    # Tuples of top skills jobs and counters
    serie_top_skills_high_ed, counts_top_skills_high_ed = top_skills_by_ed_level(base_analysis_df=get_base_analysis_df(country_argument=country_argument, list_of_clean_df= list_dfs_cleaned),
                                                                                number_skills=num_top_skills,
                                                                                ed_level=ed_levels[0],
                                                                                gender_to_eval= gender)

    serie_top_skills_medium_ed, counts_top_skills_medium_ed = top_skills_by_ed_level(base_analysis_df=get_base_analysis_df(country_argument=country_argument, list_of_clean_df= list_dfs_cleaned),
                                                                                number_skills=num_top_skills,
                                                                                ed_level=ed_levels[1],
                                                                                gender_to_eval=gender)

    serie_top_skills_low_ed, counts_top_skills_low_ed = top_skills_by_ed_level(base_analysis_df=get_base_analysis_df(country_argument=country_argument, list_of_clean_df= list_dfs_cleaned),
                                                                                number_skills=num_top_skills,
                                                                                ed_level=ed_levels[2],
                                                                                gender_to_eval=gender)

    cols = dict(zip([n for n in range(5)], ['#' + str(n) for n in range(5)]))
    # Construccion of DF of jobs
    all_dfs = [serie_top_skills_high_ed, serie_top_skills_medium_ed, serie_top_skills_low_ed]
    result_dfs = pd.concat(all_dfs, axis=1, sort=False)

    df_to_save = result_dfs.T.rename(columns=cols)

    # Construction of DF of counts
    all_counts = [counts_top_skills_high_ed, counts_top_skills_medium_ed, counts_top_skills_low_ed]
    result_counts = pd.DataFrame(all_counts)

    # Save DataFrame to csv in data/results
    m_ac.save_df_to_csv(df_to_save,
                        path=path_to_save,  # Function adds hierarchy of files
                        name=f'df_top_skills')  # Name of csv
    return df_to_save, result_counts, gender

#-------------------------------------------------------------------------------- Bonus 1: Top Skills
