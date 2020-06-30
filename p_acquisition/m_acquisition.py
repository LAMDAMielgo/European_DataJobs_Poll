from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import re
from datetime import datetime
from sklearn.preprocessing import OneHotEncoder
from functools import reduce


#-------------------------------------------------------------------------------- functions for connection t ddbb

def connect_to_table(ddbb):
    print(f'Creating DDBB connection..')
    """In case it doesn't work : 'sqlite:////home/lucia/Descargas/raw_data_project_m1.db'"""
    engine = create_engine(f'sqlite:///{ddbb}')
    connection = engine.connect()
    print(f'''Created connection with DDBB
              Table names -> \t{engine.table_names()}''')
    return engine

def ddbb_tables(engine):
    """
    Given a connection to a ddbb return tables as list
    """
    print(f'Getting table names...')

    connection = engine.connect()
    ddbb_table_names = engine.table_names()

    print(f'Done. Table names -> {ddbb_table_names}')
    return ddbb_table_names

def fetch_all_from_tables(engine, table_names_from_ddbb):
    """
    Saves each table from DDBB to a DF
    """
    list_of_df =  []
    for table in table_names_from_ddbb:
        table_df = pd.read_sql_table(table_name=table,
                                     con= engine)
        list_of_df.append(table_df)

    return list_of_df

#-------------------------------------------------------------------------------- functions for number standarization

def ageStr_to_ageNum(serie):
    """
    INPUT -> serie df[] = ['61 years old', '57 years old', '32 years old'] -> full strings
    OUPUT -> serie df[] = [ 61 57 32 45 41 1990 2000]                      -> only integers
    """
    print(f' ·· Transform strings into integers')
    serie = serie.apply(lambda x: re.sub('\D', '', x)).astype(int)
    return serie

def year_to_age(serie):
    """
    INPUT -> serie:  df[] = [ 61 57 32 45 41 1990 2000]   -> ages + years (all ints)
    OUTPUT -> serie: df[] = [ 61 57 32 45 41   30   20]   -> only ages    (all ints)
    """
    print(f' ·· Evaluating integers as [age]')
    year_db = 2016  # DB is from this year!
    serie = serie.apply(lambda x: year_db - x if int(x) > 200 else x)
    return serie

def year_update(serie):
    """
    La tabla está en 2016, hay que actualizar datos para uqe no haya incongruencias entre Age y Age_Group
    """
    print(f' ·· [year_update] doing its magic')
    year_now = datetime.today().year
    year_db = 2016
    serie = serie.apply(lambda x: (year_now - year_db) + x)
    return serie

#-------------------------------------------------------------------------------- functions for string standarization

def null_to_unknown(serie):
    """
    INPUT  -> no  high     None  medium     None  low  no
    OUTPUT -> no  high  unknown  medium  unknown  low  no
    """
    print(f' ·· Transform [None] to [Unknown]') # Data interpretation here
    serie = serie.apply(lambda x: 'unknown' if x == None else x)
    return serie

def gender_homogenization(serie):
    """
    INPUT  -> female, FeMale, Fem, male, Male
    OUTPUT ->      F,      F,   F,    M,    M
    """
    print(f' ·· Binarazing [gender]')
    serie = serie.apply(lambda x: re.sub('^f\w+|^F\w+', 'F', x))
    serie = serie.apply(lambda x: re.sub('^m\w+|^M\w+', 'M', x))
    return serie

def context_homogenization(serie):
    """
    INPUT  ->   urban  city  rural  Non-Rural Countryside  -> various types of response
    OUTPUT ->   urban  urban rural      urban       rural  -> two types of response
    ------------------------------------------------------------------------------------
    LIST OF POSSIBLE ANSWERS TAKING INTO ACCOUNT serie count values
    """
    print(' ·· Binarizing [context]')
    urban_context = ['urban', 'city', 'non-rural', 'Non-Rural']
    rural_context = ['rural', 'country', 'countryside', 'Country']

    return ['urban_context' if response in urban_context
            else 'rural_context' if response in rural_context
            else None
            for response in serie]

def split_str_at_char(str_to_split, cutter):
    """
    Separates a string into a list of strings by string (=cutter)
    --------------------------------------------------------------------------------------
    INPUT:    ['Hola que tal | soy Pepito | Hace un día estupendo']    --> list w one string
    OUTPUT:   [ 'Hola que tal', 'soy Pepito', 'Hace un día estupendo'] --> list w n string
    """
    if isinstance(str_to_split, str) and isinstance(cutter, str):
        return re.split(cutter, str_to_split)[1].capitalize()
    else:
        print('EntryError [at split_str_at_char]: wrong type of inputs')

def yes_no_to_bool(serie):
    """
    Appliable to yes/no questions with multiple formats, to transform into boolean info
    INPUT  -> YES yes Yes yES No NO nO no  -> type str
    OUTPUT ->   1   1   1   1  0  0  0  0  -> type bool
    """
    print(f' ·· Binarizing [yes/no answers]')
    serie = serie.apply(lambda x: re.sub('^y\w+|^Y\w+', '1', x))
    serie = serie.apply(lambda x: re.sub('^n\w+|^N\w+', '0', x)).astype(int)
    return serie.astype(bool)

#-------------------------------------------------------------------------------- functions for number standarization

def separate_df_to_bools(df, cols_to_separate, cols_separated):
    """
    INPUT  -> df[col].unique() = [range_1, range_2, range_3]
    OUTPUT -> df[[range_1, range_2, range_3]] with boolean responses
    """
    print(f' ·· Separating binary columns')
    df_encoder = OneHotEncoder(dtype=bool, sparse=True)
    df = pd.DataFrame(df_encoder.fit_transform(df[cols_to_separate]).toarray(), columns=cols_separated)
    return df

#---------------------------------- all of this is only for polls_info!

def get_uniqueResponses(serie, separator):
    """
    This function searches for the uniques responses in a multiple choice response
    presented as a concatenated string in one column.
    ----------------------------------------------------------------------------------
    INPUT  ->  'E | F | C | D'   'D | A'    'C'   'E | B'  --> Unsorted concat strings
    OUTPUT ->  ['E', 'F', 'C', 'D', 'A', 'B']              --> Unsorted unique strings
    """
    try:
        if isinstance(separator, str):
            list_of_all_responses = set()

            print(f' ·· Getting Unique values in [poll_column]')
            flattened_list_of_responses = reduce(lambda x, y: x + y,
                                                 [item.split(separator) for item in serie.unique()])

            for response in flattened_list_of_responses:
                if response not in list_of_all_responses:
                    list_of_all_responses.add(response)

            return list(list_of_all_responses)
        else:
            print('EntryError [at get_uniqueResponses]: wrong type of inputs')

    except:
        print('Exception at [get_uniqueResponses]')

def to_binary_matrix_of_equals(list_uniques, list_to_eval):
    """
    This returns multiple strings in an boolean form, evaluated against all options
    Making very easily to transform each list_to_eval into a DF of its own
    -------------------------------------------------------------------------------
    INPUT:   uniques [opcion 1, opcion 2, opcion 3]
           list eval [[option 1], [opcion 1, opcion 3], [opcion1, opcion 3, opcion 2]]

    OUTPUT:   matrix [[1,0,0], [1,0,1], [1,1,1]]
    """
    print(f' ··· Initiating binary matrix of  [poll_column]')

    list_uniques_lenghts = [len(i) for i in list_uniques]

    list_to_eval_iterabl = iter(list_to_eval)
    list_to_eval_lenghts = list(reduce(lambda x, y: x + y, list_to_eval))

    list_of_lists = []

    try:
        print(f'\t ···· Iterating through lists')
        for v in list_to_eval_lenghts:
            arr = [len(i) for i in next(list_to_eval_iterabl)]
            list_of_arrays = []

            for len_num in arr:
                list_of_arrays.append(np.where(np.array(list_uniques_lenghts) == len_num, 1, 0))

            list_of_lists.append(list_of_arrays)

    except StopIteration:
        pass

    print(f'\t  ··· Iteration ended. Exited loop')
    flat_arrays = [np.sum(i, axis=0) for i in list_of_lists]
    binary_matrix = [i.tolist() for i in flat_arrays]

    return binary_matrix

def multiple_choice_col_to_df(serie, separator):
    """
    Makes all the operations to return a boolean df with all the possible responses from each poll
    Nested functions : get_uniqueResponses()   to_binany_matrix_of_equals()
    """
    print(f'\n ·· Creating DFs from [poll_column]')
    poll_info_allResponses = get_uniqueResponses(serie, separator)
    graph_list_of_responses = serie.apply(lambda x: x.split(separator))
    bin_matrix = to_binary_matrix_of_equals(poll_info_allResponses, graph_list_of_responses)
    df = pd.DataFrame(bin_matrix, columns=poll_info_allResponses, dtype='bool')
    print(f' ·· DF for answers created')

    return df

#-------------------------------------------------------------------------------- functions to save outputs

def save_df_to_csv(df, path, name):
    print(f' ·· Saving df in {path} as {name}')
    path = './' + f'{path}'
    return df.to_csv(f'{path}/{name}.csv')

def save_img_to_folder():
    pass


