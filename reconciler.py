import pandas as pd
from typing import List,Dict,Callable,Tuple

def missing_records(source_df : pd.DataFrame, target_df : pd.DataFrame)->Tuple[pd.DataFrame,pd.DataFrame]:
    """
    Find records that are in source_df but not in target_df and vice versa.

    Parameters:
    - source_df (pd.DataFrame): The source DataFrame.
    - target_df (pd.DataFrame): The target DataFrame.

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame]: Two DataFrames representing records missing in the source and target DataFrames.
    """
    source_df = source_df.reset_index(drop=True).set_index(source_df.columns[0])
    target_df = target_df.reset_index(drop=True).set_index(target_df.columns[0])
    

    idx_source_not_in_target = source_df.index.difference(target_df.index)
    in_source_not_in_target = source_df.loc[idx_source_not_in_target]

    idx_target_not_in_source = target_df.index.difference(source_df.index)
    in_target_not_in_source = target_df.loc[idx_target_not_in_source]
    
    return in_source_not_in_target,in_target_not_in_source

def _output_difference(row,column, source_suffix,target_suffix)->str:
    source_value =  row[f"{column}{source_suffix}"]
    target_value = row[f"{column}{target_suffix}"]
    if source_value != target_value:
        return f"{source_value} != {target_value}"
    return ""

def _compose_transformers(transformers:List[Callable])->Callable:
    
    def composed_function(value):
        result = value
        for transformer in transformers:
            result = transformer(result)
        return result
    
    return composed_function

def find_discrepancies(source_df: pd.DataFrame,target_df: pd.DataFrame,compare_columns:List[str] = None,cols_transform:Dict[str,List[Callable]]=None)->  pd.DataFrame:
    """
    Find discrepancies between two DataFrames.

    Parameters:
    - source_df (pd.DataFrame): The source DataFrame.
    - target_df (pd.DataFrame): The target DataFrame.
    - compare_columns (List[str]): List of columns to compare. If None, all columns are considered.
    - cols_transform (Dict[str, List[Callable]]): Dictionary specifying transformers for specific columns.

    Returns:
    pd.DataFrame: DataFrame containing discrepancies between source and target DataFrames.
    """
    source_df = source_df.reset_index(drop=True).set_index(source_df.columns[0])
    target_df = target_df.reset_index(drop=True).set_index(target_df.columns[0])

    #transform the columns if cols_transform is specified
    if cols_transform:
        for column, transformers in cols_transform.items():
            # we replace the column for now, but Ideally we should created a separate transformed column
            source_df[column]=source_df[column].apply(_compose_transformers(transformers))
            target_df[column]=target_df[column].apply(_compose_transformers(transformers))
    common_entries = source_df.merge(target_df, how='inner',left_index=True,right_index=True,suffixes=('_source', '_target'))
    columns_to_compare = compare_columns if compare_columns is not None else source_df.columns
    combined_filter = pd.Series(False,index = common_entries.index)
    for column in columns_to_compare:
        column_difference_filter = common_entries[f'{column}_source'] != common_entries[f'{column}_target']
        combined_filter = combined_filter | column_difference_filter
    discrepancies = common_entries[combined_filter].dropna()
    for column in columns_to_compare:
        discrepancies[column + '_discrepancy'] = discrepancies.apply(lambda row: _output_difference(row,column,"_source","_target"),axis=1)
    return discrepancies

