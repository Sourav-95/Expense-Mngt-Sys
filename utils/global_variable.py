from config.constants import BANK_VARIABLES
from utils.logger import get_logger
logger = get_logger(__name__)

def get_input_variable(file_name: str):
    """ Validates and returns the input variables (e.g. skip_row_value, split_particular) based on the bank name inferred from the file path.
        The bank name is determined by checking if any of the keys in the BANK_VARIABLES dictionary are present 
            in the file path (case-insensitive match). 
        If a match is found, the corresponding variables are returned.
        If no match is found, an error is logged and None values are returned.
        Args:    - file_path (str): The file path from which to infer the bank name and retrieve the corresponding variables.
        Returns: A tuple containing the bank name, skip_row_value, and split_particular for the identified bank, or None values if no matching bank is found.   
    """

    if BANK_VARIABLES is None:
        logger.error("BANK_VARIABLES is not defined in config/constants.py")
        return None, None, None
    elif not isinstance(BANK_VARIABLES, dict):
        logger.error("BANK_VARIABLES should be a dictionary in config/constants.py")
        return None, None, None
    else:
        for bank in BANK_VARIABLES.keys():
            if bank in file_name.lower():
                return  bank, \
                        BANK_VARIABLES[bank]['skip_row_value'], \
                        BANK_VARIABLES[bank]['split_particular']
        
        # No matching bank found
        logger.error(f"No matching bank found in file path: {file_name}")
        return None, None, None