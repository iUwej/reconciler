
import re
from  dateutil import parser
from typing import Callable,Any

def str_compact(input : str)->str:
     """
    Remove extra spaces between words in a string.

    Parameters:
    - input_string (str): The input string with extra spaces.

    Returns:
    str: The input string with extra spaces removed.
    """
     results =  re.sub(r'\s+', ' ', input)
     return results


def canonical_date(input: str)->str:
     """
     Transform any date str into the format yyyy-mm-dd

     Parameters:
    - input_string (str): The input date str.

    Returns:
    str: The date transformed into canonical format.

     """
     parsed = parser.parse(input)
     return parsed.strftime("%Y-%m-%d")

def invoke_on(function_name : str) ->Callable:
    """
    Returns a function that accepts an object and invoke the function_name on
    the object as the receiver
    """
    
    def invoke(value)->Any:
        func = getattr(value,function_name,None)
        if callable(func):
            return func()
        else:
            raise AttributeError(f"{function_name} is not a callable attribute of {value}")
    return invoke
