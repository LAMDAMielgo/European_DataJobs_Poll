import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rc

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
        top_colors = ['darkcyan', 'cadetblue', 'cyan', 'lightseagreen', 'lightskyblue']
    else:
        top_colors = ['firebrick', 'red', 'tomato', 'indianred', 'lightred']

    # stack_bars
    top_1 = plt.bar(ind, counts_1, width, bottom=0, color= top_colors[0])
    top_2 = plt.bar(ind, counts_2, width, bottom=counts_1, color= top_colors[1])
    top_3 = plt.bar(ind, counts_3, width, bottom=[i + j for i, j in zip(counts_1, counts_2)], color= top_colors[2])
    top_4 = plt.bar(ind, counts_4, width, bottom=[i + j + k for i, j, k in zip(counts_1, counts_2, counts_3)],color= top_colors[3])
    top_5 = plt.bar(ind, counts_5, width, bottom=[i + j + k + l for i, j, k, l in zip(counts_1, counts_2, counts_3, counts_4)], color= top_colors[4])

    plt.ylim(0, 45)
    # Legend for X and Y
    plt.title(f'VARIATION OF TOP SKILLS BY EDUCATION. GENDER = {gender}')
    plt.xticks(ind, ('high_ed', 'medium_ed', 'low_ed'))
    plt.legend((top_1[0], top_2[0], top_3[0], top_4[0], top_5[0]), ('#1', '#2', '#3', '#4', '#5'))

    #Save to data/results
    fig = plt.gcf()
    fig.set_size_inches(7, 20)
    fig.savefig(f'./data/results/{file_name}.png', dpi=100)


def get_misingno_bars(df):
    pass