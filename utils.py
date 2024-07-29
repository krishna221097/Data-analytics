"""
utils.py

Provides utility functions to setup logging, read and write data from files.
"""


import logging
import pandas as pd
import openpyxl


def setup_logging():
    """
    Setup logging to print information to the console. 
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(lineno)d:\n%(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    c_handler.setFormatter(formatter)
    logger.addHandler(c_handler)
    return logger

logger = setup_logging()

def write_to_file(df:pd.DataFrame, name, file_type='excel', index=False, logger=None) -> None:
    """
    Utility function to write data to csv file (extension must be provided).

    Args:
        df (pd.DataFrame): Dataset to be written to csv.
        name (str): Name to be assigned to file.
        file_type (str): Choose between 'excel' and 'csv' format.
        index (bool): Include index in final dataset.
        logger (<logging> object): assign existing logger (see utils.py) to function to enable log messages.

    Returns:
        None: Confirmation and filepath written to console.
    """
    if not name: raise ValueError('No name provided.')
    if not isinstance(name,str): raise TypeError('Incorrect data type provided for file name.')
    
    try:
        if file_type=='csv':
            df.to_csv(f'output/{name}', index=index)
            if logger:
                logger.debug(f'Completed writing {name} to CSV file. Check output/{name}.')
        elif file_type=='excel':
            df.to_excel(f'output/{name}', index=index)
            if logger:
                logger.debug(f'Completed writing {name} to Excel file. Check output/{name}.')
    except Exception as e:
        logger.error(e)
    

def read_from_excel(file_name:str) -> pd.DataFrame:
    """
    Utility function to read data from Excel. File must be stored in '/input/'
    
    Args: 
        file_name (str): single file name (extension must be provided).
    
    Returns:
        df (pd.DataFrame): data stored inside filename provided.
    """
    location = 'input/'
    df = pd.read_excel(f"{location}{file_name}")
    return df
