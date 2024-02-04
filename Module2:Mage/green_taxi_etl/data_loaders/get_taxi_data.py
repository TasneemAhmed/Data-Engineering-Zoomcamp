import pandas as pd

from mage_ai.io.file import FileIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    """
    parsing_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    #using global variable of dataset path directory
    path_dir = kwargs['path_dir']
    
    df_oct = pd.read_csv(path_dir+'/green_tripdata_2020-10.csv', parse_dates = parsing_dates)
    df_nov = pd.read_csv(path_dir+'/green_tripdata_2020-11.csv', parse_dates = parsing_dates)
    df_dec = pd.read_csv(path_dir+'/green_tripdata_2020-12.csv', parse_dates = parsing_dates)
    
    #concat rows of above dataframes (like union in sql)
    last_quarter_data = pd.concat([df_oct, df_nov, df_dec], axis=0,ignore_index = True)

    #Save loaded data into file
    last_quarter_data.to_csv(path_dir+'/last_quarter_data.csv')
    #print(last_quarter_data.dtypes)

    return last_quarter_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
