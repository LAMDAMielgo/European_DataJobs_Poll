import pandas as pd
from functools import reduce

from p_acquisition import m_acquisition as m_ac
#-------------------------------------------------------------------------------- Evals country_argparse
def country_argparse_eval(country_argument, list_to_search):
    """
    Evaluates whether argument exists in df_country_info[country_names]
    """
    if country_argument in list_to_search:
        def_country_argument = country_argument
        return def_country_argument
    else:
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
    return country_argparse_filter(country_argument, df_final)

def get_percentages_gender_by_job(base_analysis_df):
    """
    INPUT  -> from all cleaned dfs, joined useful columns by uuid
    OUTPUT -> csv with percentages by country, job and gender
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

    # Getting DF w/ top skills and DF w/counts
    df_tk = df_high_ed_skills_gender[filtr].reset_index().head(number_skills)
    df_tk = df_tk.rename(columns={filtr: ed_level}).drop(columns='index')
    counts = counts_by_gender[:number_skills]

    # returns tuple
    return df_tk, counts

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

#-------------------------------------------------------------------------------- Bonus 2: Poll Information
def get_df_poll_filtered_by_gender(tuple_separated_polls, poll_to_extract, base_analysis_df):
    """
    INPUT  ->  [poll_1, poll_2, poll_3, poll_4] + 'poll_3'    -> data cleaned as extracted
    OUTPUT ->  in poll_3, list of trues responses by gender   -> data for visualization
    """
    # Function variables
    possible_cols = tuple_separated_polls[1]
    list_separated_polls_df = tuple_separated_polls[0]
    gender = ('M', 'F') # this needs to change, buuut

    try:
        if poll_to_extract in possible_cols:
            index_of_df = possible_cols.index(poll_to_extract)
            df_poll = list_separated_polls_df[index_of_df]

            # important columns
            cols_poll = list(df_poll.columns)

            df_joined = base_analysis_df[['uuid', 'gender']].join(df_poll, on=None, how='left', sort=False)

            # getting data for visualize
            list_of_lists = []

            for gender_name in gender:
                list_of_counts = []

                for i in range(len(cols_poll)):
                    list_of_counts.append(df_joined[df_joined['gender'] == gender_name][cols_poll[i]].value_counts())

                list_of_trues = [list_of_counts[i].iloc[1] for i in range(len(list_of_counts))]
                list_of_lists.append(list_of_trues)

            return dict(zip(gender, list_of_lists)), cols_poll, index_of_df

        else:
            raise ValueError

    except ValueError:
        print('The entry [poll_to_extract] is not correct')