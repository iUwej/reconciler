
import argparse
import pandas as pd
import os
import traceback
import reconciler

from typing import List,Tuple
from transform_utils import canonical_date,str_compact,invoke_on

def ensure_path_exists(file_path :str):
    if not os.path.exists(file_path):
        raise Exception(f"{file_path} is not a valid file path")

def run(source_path: str, target_path: str,output_path: str,compare_cols:List[str],transfomers:List[Tuple[str,str]]):
    ensure_path_exists(source_path)
    ensure_path_exists(target_path)

    source_df = pd.read_csv(source_path,header=0,dtype={0:str})
    target_df = pd.read_csv(target_path,header=0,dtype={0:str})

    source_cols = source_df.columns
    if compare_cols and not all([ col in source_cols for col in compare_cols]):
        raise Exception(f"some compare columns are missing in the source file header columns")
    
    col_transformers = {}
    if transfomers:
        for entry in transfomers:
            col,dataType = entry
            if not col in source_cols:
                raise Exception(f"column {col} specified on the transfomer option {col}:{dataType} is not among the source file columns")
            if dataType == 'date':
                col_transformers[col] = [canonical_date]
            elif dataType == 'str':
                col_transformers[col] =  [str_compact,invoke_on("strip"),invoke_on("lower")]
            else:
                raise Exception(f"Unsupported data type {dataType} on transformer {col}:{dataType}. Allowed values are str and date")
        
    missing_in_target, missing_in_source = reconciler.missing_records(source_df,target_df)
    print(missing_in_target)
    # call with None if empty
    compare_columns = compare_cols if compare_cols else None
    col_transformers = col_transformers if col_transformers else None
    
    discrepancies = reconciler.find_discrepancies(source_df,target_df,compare_columns,col_transformers)
    with open(output_path, 'w', newline='') as out_file:
        out_file.write("\n*************Missing in Target********************\n")
        missing_in_target.to_csv(out_file,mode='a',index=True)
        out_file.write("\n\n*************Missing in Source ********************\n")
        missing_in_source.to_csv(out_file,mode='a',index=True)
        out_file.write("\n\n*************Field Discrepancy ********************\n")
        discrepancies.to_csv(out_file,mode='a',index=True)
    print("Reconciliation completed:")
    print("Records missing in target: ",len(missing_in_target))
    print("Records missing in source",len(missing_in_source))
    print("Records with field disrepancies",len(discrepancies))

def parse_key_value_pairs(value):
    tokens = value.split(':')
    if len(tokens) != 2:
        raise argparse.ArgumentTypeError(f"Invalid format: {value}. Must be in the form 'STRING:STRING'")
    return (tokens[0],tokens[1])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="csv_reconciler",
        description="A tool that reads in two csvs, reconcile the records and produce a report detailing the difference between the two",
        usage="""
        Basic usage:    python csv_reconciler.py -s source.csv -t target.csv -o out.csv
        Specify comparison columns:   python csv_reconciler.py -s source.csv -t target.csv  -o out.csv --cmp_columns Name Amount
        Transfomers example:    python csv_reconciler.py -s source.csv -t target.csv  -o out.csv --cmp_columns Name Amount --transform Date:date Name:str
        """,
        epilog="""
        """
        )
    parser.add_argument("-s","--source",help="Source file path",required=True)
    parser.add_argument("-t","--target",help="Target file path",required=True)
    parser.add_argument("-o","--output",help="Path to write the reconcilation report",required=True)
    parser.add_argument("-cc","--cmp_columns",help="The columns to use to find discrepancies between records ",metavar="STRING",type=str,nargs='+',required=False)
    parser.add_argument("--transform",
                        help="""A list of columns and their data types in the form COLUMN_NAME:type COLUMN_NAME1:type  to apply the default trasformations for that type.
                        For example to make all dates in a column standard yyyy-mm-dd(2023-01-23) you would supply COLUMN_NAME:date.
                        To make a string value  standard by removing extra spaces between words, trimming ends and using same case(lower case) , supply COLUMN_NAME:str
                        Supported types for this option are date and str but this range can be extended by implementing the necessary transformers for the desired type.
                        """,
                        metavar="STRING:STRING",
                        type=parse_key_value_pairs,
                        nargs='+',
                        required=False,
                        action='store',
                        dest='transfomers'
                        )
    args = parser.parse_args()
    try:
        run(args.source,args.target,args.output,args.cmp_columns,args.transfomers)
    except Exception as e:
        print(f"\ncsv_reconciler ERROR: {e}")
        print(f"\n\n***********Debug Info***************")
        traceback.print_exc()