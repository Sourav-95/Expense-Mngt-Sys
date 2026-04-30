import sys
from io import BytesIO
import pandas as pd
from utils.global_variable import get_input_variable
from config.constants import BANK_VARIABLES, \
                                EXPECTED_COLUMN, \
                                THRESHOLD_NULL_FOR_ROW
from utils.logger import get_logger
logger = get_logger(__name__)

import warnings
warnings.filterwarnings("ignore", category=UserWarning)



def read_file_from_gdrive(file_name, iOByte_from_gdrive):
    """This function reads the file from gdrive"""

    bank, skip_row_value, _ = get_input_variable(file_name)

    try:
        data = pd.read_excel(iOByte_from_gdrive, engine='xlrd', skiprows=skip_row_value)
        logger.info(f"Data loaded as dataframe file: `{file_name}`. Row: {data.shape[0]}, Columns: {data.shape[1]}.")
    except Exception as e:
        logger.error(f"Error occurred while reading the file from gdrive: {e}")
        return None
    return data

def data_cleaner(data:pd.DataFrame, file_name, expected_column=EXPECTED_COLUMN, null_threshold_for_row=THRESHOLD_NULL_FOR_ROW) -> pd.DataFrame:
    """This function performs the data cleaning operations on the dataframe read from gdrive.
    The operations include:
    1. Dropping all null rows where all record are null
    2. Counting the no.of null in a record to delete those records where occurence is more than 2
    3. Convert all columns to lower
    4. Rename few columns for similar one
    5. Filter out rows where 'Name' contains '*' or 'Generated'
    6. Change Data-Types, handling errors for numeric columns
    7. Trimming the required columns only
    Args:    - data (pd.DataFrame): The input dataframe to be cleaned
            - expected_column (list): The list of expected column names to be used for renaming
            - null_threshold_for_row (int): The threshold for the number of null values in a row to determine if the row should be dropped
    Returns: The cleaned dataframe after performing all the above operations.
    """
    df = data.copy()

    try:
        # 1. Dropping all null rows where all record are null
        df = df.dropna(how='all')

        # 2. Counting the no.of null in a record to delete those records where occurence is more than 2
        df['null_count'] = df.isnull().sum(axis=1)
        df = df[df['null_count'] <= null_threshold_for_row]

        # 3. Convert all columns to lower
        df.columns = [col.lower() for col in df.columns]

        # 4. Rename few columns for similar one
        for col in df.columns:
            if expected_column[0] in col:
                df.rename(columns={col:'date'}, inplace=True)
            elif expected_column[1] in col:
                df.rename(columns={col:'particulars'}, inplace=True)
            elif expected_column[2] in col:
                df.rename(columns={col:'dr'}, inplace=True)
            elif expected_column[3] in col:
                df.rename(columns={col:'cr'}, inplace=True)

        # 5. Filter out rows where 'Name' contains '*' or 'Generated'
        df = df[~df['date'].str.contains(r'\*', regex=True)]
        df = df[~df['date'].str.contains('Generated', na=False)]

        # 6. Change Data-Types, handling errors for numeric columns
        # Convert 'dr' and 'cr' to numeric, coercing errors to NaN
        df['dr'] = pd.to_numeric(df['dr'], errors='coerce')
        df['cr'] = pd.to_numeric(df['cr'], errors='coerce')

        # Convert 'date' to datetime
        # df['date'] = pd.to_datetime(df['date'],dayfirst=True ,errors='coerce') --- IGNORE : Changed as caused error---
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

        # 7. Trimming the required columns only
        df = df[['date', 'particulars', 'dr', 'cr']]

        # 8. Adding bank name column
        df['bank'] = file_name.split('_')[0].lower()

        logger.info(f"Total Length of data after cleaning : {df.shape}")
        logger.info(f"Column in dataframe : {df.columns}")
    except Exception as e:
        logger.error(f"An error occurred while cleaning the data: {e}")

    return df