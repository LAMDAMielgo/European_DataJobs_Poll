import matplotlib.pyplot as plt
import numpy as np
import os
from zipfile import ZipFile

from p_reporting import m_reporting as m_rep
from p_analysis import m_analysis as m_an
#-------------------------------------------------------------------------------- global variables
PATH_TO_SAVE_CSV = './data/results/'
ZIP_FILE_NAME = 'pipeline_jobs_gender_results'
#-------------------------------------------------------------------------------- creating png viz
def get_stacked_bar_plot(tuple_top_skills):
    """Saves visualization"""

    # Variables
    gender = tuple_top_skills[2]
    file_name = f'visualizacion_{gender}_distribution_top_skills'

    Number_of_stacks = tuple_top_skills[0].shape[0] # shape is [5x3]
    ind = np.arange(Number_of_stacks)
    width = 0.35

    # list of values
    counts_1 = list(tuple_top_skills[1].iloc[:, 0])
    counts_2 = list(tuple_top_skills[1].iloc[:, 1])
    counts_3 = list(tuple_top_skills[1].iloc[:, 2])
    counts_4 = list(tuple_top_skills[1].iloc[:, 3])
    counts_5 = list(tuple_top_skills[1].iloc[:, 4])


    # list of colors
    top_colors = []
    if gender == 'M':
        top_colors = ['#22577a', '#38a3a5', '#57cc99', '#80ed99', '#c7f9cc']
    elif gender == 'F':
        top_colors = ['#f08080', '#f4978e', '#f8ad9d', '#fbc4ab', '#ffdab9']

    # stack_bars
    top_1 = plt.bar(ind, counts_1, width, bottom=0, color= top_colors[0])
    top_2 = plt.bar(ind, counts_2, width, bottom=counts_1, color= top_colors[1])
    top_3 = plt.bar(ind, counts_3, width, bottom=[i + j for i, j in zip(counts_1, counts_2)], color= top_colors[2])
    top_4 = plt.bar(ind, counts_4, width, bottom=[i + j + k for i, j, k in zip(counts_1, counts_2, counts_3)],color= top_colors[3])
    top_5 = plt.bar(ind, counts_5, width, bottom=[i + j + k + l for i, j, k, l in zip(counts_1, counts_2, counts_3, counts_4)], color= top_colors[4])

    plt.ylim(0, 150)
    # Legend for X and Y
    plt.title(f'VARIATION OF TOP SKILLS BY EDUCATION. GENDER = {gender}')
    plt.xticks(ind, ('high_ed', 'medium_ed', 'low_ed'))
    plt.legend((top_1[0], top_2[0], top_3[0], top_4[0], top_5[0]), ('#1', '#2', '#3', '#4', '#5'))

    #Save to data/results
    fig = plt.gcf()
    fig.set_size_inches(10, 20)
    fig.savefig(f'{PATH_TO_SAVE_CSV}{file_name}.png', dpi=100)

    # Closing plt
    plt.clf()
    plt.cla()
    plt.close()

def stacked_bar_graph(tuple_filtr_analysis):

    # Function variables
    possible_titles = ['BASIC INCOME AWARENESS', 'BASIC INCOME EFFECT',
                     'BASIC INCOME VOTE', 'BASIC INCOME ARGUMENTS AGAINST', 'BASIC INCOME ARGUMENTS FOR']

    bars_dict = tuple_filtr_analysis[0]
    x_bars_names = tuple_filtr_analysis[1]
    num_title = tuple_filtr_analysis[2]
    # set height of bar
    bars1 = list(bars_dict.values())[0]
    bars2 = list(bars_dict.values())[1]

    # set width of bar
    barWidth = 0.45
    barNums = len(bars1)

    # Set position of bar on X axis
    r1 = np.arange(barNums)
    r2 = [x + barWidth for x in r1]

    # Make the plot
    plt.bar(r1, bars1, color='#22577a', width=barWidth, edgecolor='white', label='var1') # gender1
    plt.bar(r2, bars2, color='#f08080', width=barWidth, edgecolor='white', label='var2') # gender2

    # Add xticks on the middle of the group bars
    plt.xlabel(possible_titles[num_title], fontweight='bold')
    plt.xticks([r + barWidth for r in range(barNums)], x_bars_names)
    plt.xticks(rotation=90)
    plt.legend(bars_dict.keys())

    #Save to data/results
    fig = plt.gcf()
    fig.set_size_inches(15, 20)
    fig.savefig(f'{PATH_TO_SAVE_CSV}/visualization_{possible_titles[num_title].capitalize()}.png', dpi=100)

    plt.clf()
    plt.cla()
    plt.close()
#-------------------------------------------------------------------------------- running all and saving in local

def distribution_top_skills(country_arg, top_skills_to_show, list_df, genders):
    m_rep.get_stacked_bar_plot(m_an.get_df_top_skills(country_arg, top_skills_to_show, list_df, gender= genders[0]))
    m_rep.get_stacked_bar_plot(m_an.get_df_top_skills(country_arg, top_skills_to_show, list_df, gender= genders[1]))

def distribution_BI_arguments(country_arg, df_info_cleaned, df_polls_cleaned):
    # Variables:
    polls_to_viz = ['question_bbi_2016wave4_basicincome_argumentsagainst', 'question_bbi_2016wave4_basicincome_argumentsfor']

    stacked_bar_graph(m_an.get_df_poll_filtered_by_gender(df_polls_cleaned,
                                        poll_to_extract=polls_to_viz[0],
                                        base_analysis_df=m_an.get_base_analysis_df(country_arg, df_info_cleaned)))

    stacked_bar_graph(m_an.get_df_poll_filtered_by_gender(df_polls_cleaned,
                                        poll_to_extract=polls_to_viz[1],
                                        base_analysis_df=m_an.get_base_analysis_df(country_arg, df_info_cleaned)))

#-------------------------------------------------------------------------------- saving to ZipFile

def save_ALL_to_Zip(arg):
    complete_path = f'./.{PATH_TO_SAVE_CSV}'
    os.chdir(complete_path)
    dirs = os.listdir(complete_path)

    with ZipFile(f'{ZIP_FILE_NAME}_{arg}.zip', "w") as zipObj:
        for file in dirs:
            zipObj.write(os.path.join(file))
        zipObj.close()
