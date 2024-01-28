import pytest
import pandas as pd

import reconciler
from transform_utils import str_compact, canonical_date,invoke_on

BASIC_SOURCE_DATA =  {
    'ID': ['001', '002', '003'],
    'Name': ['John Doe', 'Jane Smith', 'Robert Brown'],
    'Date': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'Amount': [100.00, 200.50, 300.75]
}

BASIC_TARGET_DATA = {
    'ID': ['001', '002', '004'],
    'Name': ['John Doe', 'Jane Smith', 'Emily White'],
    'Date': ['2023-01-01', '2023-01-04', '2023-01-05'],
    'Amount': [100.00, 200.50, 400.90]
}


def test_missing_records_should_find_the_missing_entries_between_two_datasets():
    source_df = pd.DataFrame(BASIC_SOURCE_DATA)
    target_df = pd.DataFrame(BASIC_TARGET_DATA)
    
    missing_in_target, missing_in_source = reconciler.missing_records(source_df,target_df)
    assert len(missing_in_target) == 1, "There should be only one record in source not in target"
    assert len(missing_in_source) == 1, "There should be only one record in target not in source"

    assert missing_in_target.index[0] == '003', "Missing record in target should have ID 3"
    assert missing_in_source.index[0] == '004', "Missing record in source should have ID 4"


def test_find_discrepancies_should_find_discrepancies_between_records_with_similar_ID():
    source_df = pd.DataFrame(BASIC_SOURCE_DATA)
    target_df = pd.DataFrame(BASIC_TARGET_DATA)
    
    discrepancies = reconciler.find_discrepancies(source_df,target_df)
    assert len(discrepancies) == 1, "There should be only one discrepancie between the two datasests"
    assert discrepancies.index[0] ==  '002', "Record with discrepancy ID should be 002"

    #we test only the Date value has a discrepancy
    
def test_find_discrepancies_should_ignore_non_relevant_columns_when_compare_columns_are_provided():
    source_df = pd.DataFrame(BASIC_SOURCE_DATA)
    target_df = pd.DataFrame(BASIC_TARGET_DATA)
    relevant_columns = ['Name','Amount']
    
    discrepancies = reconciler.find_discrepancies(source_df,target_df,compare_columns=relevant_columns)

    assert len(discrepancies) == 0, "If we ignore the date column, then all records should have no discrepancies"

def test_find_discrepancies_should_normalize_columns_when_cols_transform_is_specified():
    source_data =  {
    'ID': ['001', '002', '003'],
    'Name': ['   John            Doe', 'Jane Smith', 'Robert Brown'], # add extra spaces to name
    'Date': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'Amount': [100.00, 200.50, 300.75]
    }
    
    target_data  = {
    'ID': ['001', '002', '004'],
    'Name': ['john doe', 'Jane Smith', 'Emily White'], # write name in lower case 
    'Date': ['01/01/2023', '2023-01-04', '2023-01-05'], # change the date format for the first record
    'Amount': [100.00, 200.50, 400.90]
    }

    source_df = pd.DataFrame(source_data)
    target_df = pd.DataFrame(target_data)

    transfomers = {
        'Name': [str_compact,invoke_on("strip"),invoke_on("lower")],
        'Date': [canonical_date]
    }
    discrepancies = reconciler.find_discrepancies(source_df,target_df,cols_transform=transfomers)
    assert len(discrepancies) == 1, "Only one record should have a discrepancies"
    assert discrepancies.index[0],"Record with discrepancy ID should be 002"

