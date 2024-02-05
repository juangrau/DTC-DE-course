import re

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    print(name)
    return name

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    data = data[data['passenger_count']>0]
    data = data[data['trip_distance']>0]
    column_names = data.columns
    new_col_names = []
    for col in column_names:
        new_col_names.append(camel_to_snake(col))
    data.columns = new_col_names
    print(data['vendor_id'].unique())

    return data


@test
def test_output(output, *args) -> None:
    """
    Objective:
    vendor_id is one of the existing values in the column (currently)
    passenger_count is greater than 0
    trip_distance is greater than 0

    """
    #assert output is not None, 'The output is undefined'
    assert output.columns.isin(['vendor_id']).sum() > 0, 'The column vendor_id exists in the DataFrame'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    
