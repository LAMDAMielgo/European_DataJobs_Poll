import pandas as pd

# acquisition functions
## all in here

def acquire_raw_data():
    print(f' -> Getting data ...')
    data = pd.read_csv('./data/raw/raw_data_project_m1.db')
    print(f' -> Getting raw_data Done!')
    return data

def saving_processed_data():
    # print(f' -> Saving..')
    # print(f' -> Getting raw_data Done!')
    pass



##########################

