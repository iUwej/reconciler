## Running the csv reconciler

### Set up
1) Install python: Ensure you have python 3.8 installed
2) Setup a virtual environment(optional): It's recommeded to use a virtual environment to isolate the environment of an application. Follow this guideline [link](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/) to set up a venv.
3) Install depedencies: While in the reconciler project path run the command <br>`pip install -r requirements.txt`

### Running the reconciler
1) While in the reconciler project path run the command: <br>
    `python csv_reconciler.py -h`

    You will be presented with a help prompt similar to the one shown below: <br>
    ```
    (.venv) ➜  reconciler git:(master) ✗ python csv_reconciler.py -h            
    usage: 
        Basic usage:    python csv_reconciler.py -s source.csv -t target.csv -o out.csv
        Specify comparison columns:   python csv_reconciler.py -s source.csv -t target.csv  -o out.csv --cmp_columns Name Amount
        Transfomers example:    python csv_reconciler.py -s source.csv -t target.csv  -o out.csv --cmp_columns Name Amount --transform Date:date Name:str
        
        A tool that reads in two csvs, reconcile the records and produce a report detailing the difference between the two
        
        optional arguments:
        -h, --help            show this help message and exit
        -s SOURCE, --source SOURCE
                        Source file path
  -     t TARGET, --target TARGET
                        Target file path
        -o OUTPUT, --output OUTPUT
                        Path to write the reconcilation report
        -cc STRING [STRING ...], --cmp_columns STRING [STRING ...]
                        The columns to use to find discrepancies between records
            --transform STRING:STRING [STRING:STRING ...]
                            A list of columns and their data types in the form COLUMN_NAME:type     COLUMN_NAME1:type to apply the default trasformations for that type. For example to make all dates in a
                            column standard yyyy-mm-dd(2023-01-23) you would supply COLUMN_NAME:date. To make a string value standard by removing extra spaces between words, trimming ends and using
                            same case(lower case) , supply COLUMN_NAME:str Supported types for this option are date and str but this range can be extended by implementing the necessary transformers
                            for the desired type.```

    ### Running Tests
    To run the unit tests, run the following command  on th project's root<br>
    `pytest`