import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #using global variable of dataset path directory
    path_dir = kwargs['path_dir']
    
    """
    ######################### Transformations needed ############################################
        1. filter rows with passanger_count & trip_distance > 0
        2. new column from 'lpep_pickup_datetime' with date type
        3. convert dataframe columns from CamelCase to snake_case
    """
    #1. filter rows with passanger_count & trip_distance > 0
    data = data[(data.passenger_count >0) & (data.trip_distance > 0)]

    #2. create new column give only date from datetime column 'tpep_pickup_datetime' with string format
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #3. regex with str.replace to detect the lowercase-UPPERCASE shifts and insert a _, then str.lower
    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )
    
    #save transformed data into file
    data.to_csv(path_dir+'/transformed_data.csv')

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
