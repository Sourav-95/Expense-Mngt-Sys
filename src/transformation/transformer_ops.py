import pandas as pd
from pandas.tseries.offsets import MonthBegin
import numpy as np
from rapidfuzz import process, fuzz
from utils.global_variable import get_input_variable
from config.constants import BANK_VARIABLES,SUBCATEGORY_MAPPING, CATEGORY_FRM_SUBCATEGORY
from utils.logger import get_logger
logger = get_logger(__name__)

def parse_split_data(row, bank):
    """ This function takes a row of the DataFrame and the bank name as input, 
        and returns a Series with the parsed values for 'transaction_type', 'transaction_no', 'payment_to', and 'raw_category'
        based on the logic defined for each bank. 
        The logic is determined by the length of the split 'particulars' and the specific bank's formatting rules. 
        The function uses a dictionary to store the results, which allows for easy handling of missing values and 
        different formats across banks.
        Args:
            row (pd.Series): A row of the DataFrame containing the 'split' and 'len_split' columns.
            bank (str): The name of the bank to determine the parsing logic.
        Returns:
            pd.Series: A Series containing the parsed values for 
                    'transaction_type', 'transaction_no', 'payment_to', and 'raw_category'.
    """

    try:
        data = row['split']
        length = row['len_split']

        # Initialize a dictionary with default values
        res = {
            'transaction_type': None,
            'transaction_no': None,
            'payment_to': None,
            'raw_category': None
        }
        if bank in BANK_VARIABLES.keys():
            if bank == 'axis':
                # Use your logic to fill the dictionary
                if length == 5:
                    res['transaction_type'] = data[0]
                    res['raw_category']     = data[4]

                elif length == 6:
                    res['transaction_type'] = data[0]
                    res['transaction_no']   = data[2]
                    res['payment_to']       = data[3]
                    res['raw_category']     = data[4]

                elif length == 7:
                    res['transaction_type'] = data[0]
                    res['transaction_no']   = data[2]
                    res['payment_to']       = data[3]
                    res['raw_category']     = data[5]


            elif bank == 'hdfc':
                # Example: HDFC might have different lengths or positions
                res['transaction_type'] = data[0]
                if length == 5:
                    res['transaction_no']       = data[3]
                    res['raw_category']         = data[4]
        # # Add more bank-specific parsing logic as needed
        # elif bank == 'icici' 
        else:
            logger.warning(f"No specific parsing logic defined for bank `{bank}`. Returning None for all parsed fields.")
    except Exception as e:
        logger.error(f"Error occurred while parsing split data for bank `{bank}`: {e}")
    return pd.Series(res)

def apply_is_manual_logic(df, bank_name):
    """ This function applies the logic to determine if a transaction is manual or not 
        based on the bank name and the content of the 'cr' column.
        The logic is as follows:
        - If the 'cr' column is not null, then 'is_manual' is set to 'Y' regardless of the bank.
        - For HDFC bank, 'is_manual' is always 'Y'.
        - For Axis bank, 'is_manual' is 'Y' if the length of the split 'particulars' is 1 or 5, otherwise it is 'N'.
        - For all other cases, 'is_manual' is set to 'N'.
        Args:
            df (pd.DataFrame): The input DataFrame containing the transaction data.
            bank_name (str): The name of the bank to determine the logic for 'is_manual'.
        Returns:
            pd.DataFrame: The DataFrame with the 'is_manual' column added based on the defined logic.
    """
    bank = str(bank_name).lower().strip()

    try: 
        # Define your conditions
        conditions = [
            (df['cr'].notnull()),                             # New condition: if 'cr' is not null, 'is_manual' is 'Y'
            (bank == 'hdfc'),                                  # HDFC: Always Y
            (bank == 'axis') & (df['len_split'].isin([1, 5]))  # Axis: Y if length is 1 or 5
        ]

        # Corresponding choices for each condition
        choices = ['Y', 'Y', 'Y']

        # Apply logic; everything else defaults to 'N'
        df['is_manual'] = np.select(conditions, choices, default='N')
    except Exception as e:
        logger.error(f"Error occurred while applying is_manual logic for bank `{bank_name}`: {e}")
    return df


def data_preprocessor(data: pd.DataFrame, bank: str) -> pd.DataFrame:
    """ This function performs the data preprocessing operations on the dataframe after cleaning.
        The operations include:
        1. Splitting the 'particulars' column into a list based on a delimiter specific to the bank.
        2. Removing empty items from the split list.
        3. Counting the length of the split list to determine the number of elements.
        4. Applying logic to determine if a transaction is manual or not based on the bank and the length of the split list.
        5. Parsing the split data to extract 'transaction_type', 'transaction_no', 'payment_to', and 'raw_category' based on the bank's formatting rules.
        Args:
            data (pd.DataFrame): The input dataframe to be preprocessed.
            bank (str): The name of the bank to determine the preprocessing logic.
        Returns: The preprocessed dataframe with new columns for 
                'transaction_type', 'transaction_no', 'payment_to', 'raw_category', and 'is_manual'.
    """
    df: pd.DataFrame = data.copy()
    bank_result = get_input_variable(bank)
    
    # Validate that we got valid bank info
    if bank_result[0] is None or bank_result[1] is None or bank_result[2] is None:
        logger.error(f"Could not get input variables for bank: {bank}")
        return df
    
    bank_name: str = bank_result[0]
    skip_row_value: int = bank_result[1]
    split_particular: str = bank_result[2]
    
    try:
        df['split'] = df['particulars'].str.split(split_particular)

        # Removes items that are empty OR just contain spaces
        df['split'] = df['split'].apply(lambda x: [item for item in x if item.strip() != ''])

        # counting the len of splitted particular
        df['len_split'] = df['split'].str.len()

        df = apply_is_manual_logic(df, bank_name)

        # Define the column names we want to create
        target_cols = ['transaction_type', 'transaction_no', 'payment_to', 'raw_category']

        # Apply the function and assign directly to the list of column names
        df[target_cols] = df.apply(parse_split_data, axis=1, bank=bank_name)
        logger.info(f"Total Length of data after preprocessing : {df.shape}")
    except Exception as e:
        logger.error(f"An error occurred while preprocessing the data: {e}")
    return df

def clean_raw_category(df: pd.DataFrame) -> pd.DataFrame:
    """ This function performs generic cleaning operations on the 'raw_category' column of the DataFrame.
        The operations include:
        1. Converting all values in the 'raw_category' column to lowercase to ensure uniformity.
        2. Replacing common substrings in the 'raw_category' values with standardized category names based on a predefined mapping. This helps in reducing the number of unique categories and grouping similar categories together.
        3. Handling None or null values in the 'raw_category' column by filling them with a default value such as 'unspecified' to avoid issues in downstream processing.
        Args:
            df (pd.DataFrame): The input DataFrame containing the 'raw_category' column to
        Returns: The DataFrame with the cleaned 'raw_category' column, where values are standardized and nulls are handled appropriately.

    """

    df_cleaned = df.copy()

    # Define mapping for generic replacements with all keys in lowercase
    try:
        # Convert to lowercase and apply mapping
        df_cleaned['raw_category'] = df_cleaned['raw_category'].str.lower().replace(SUBCATEGORY_MAPPING)

        # Handle None values by converting to 'unspecified' or similar if needed
        df_cleaned['raw_category'] = df_cleaned['raw_category'].fillna('unspecified')

        logger.info(f"Completed Cleaning of 'raw_category'")
    except Exception as e:
        logger.error(f"An error occurred while cleaning the 'raw_category' column: {e}")
    return df_cleaned

def apply_raw_category_transformation(df: pd.DataFrame) -> pd.DataFrame:
    df_transformed = df.copy()

    try:
        # Normalize both columns
        df_transformed['raw_category'] = (
            df_transformed['raw_category']
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df_transformed['payment_to'] = (
            df_transformed['payment_to']
            .astype(str)
            .str.strip()
            .str.lower()
        )

        categories_to_replace = [
            'upi', 'upi mand', 'mandatee', 'payment',
            'pay to', 'recurring', 'return', 'static'
        ]

        mask = df_transformed['raw_category'].isin(categories_to_replace)

        # Replace AND keep lowercase
        df_transformed.loc[mask, 'raw_category'] = df_transformed.loc[mask, 'payment_to']

        logger.info(f"`raw_category` transformation applied based on `payment_to` for specific categories.")
    except Exception as e:
        logger.error(f"An error occurred while applying raw category transformation: {e}")
    return df_transformed

def capitalize_raw_category(df: pd.DataFrame) -> pd.DataFrame:
    """Capitalizes the first letter of each word in the 'raw_category' column."""
    df_capitalized = df.copy()
    df_capitalized['raw_category'] = df_capitalized['raw_category'].str.capitalize()
    logger.info(f"`raw_category` first letter capitalized")
    return df_capitalized



def modify_salaried_record(data: pd.DataFrame):
    df = data.copy()

    # 1. Identify rows where 'raw_category' contains '20532356' for salary
    mask = df['raw_category'].str.contains('20532356', na=False)

    # 2. Update the timestamp to the 1st of the next month
    # This automatically handles leap years and different month lengths
    df.loc[mask, 'date'] = pd.to_datetime(df.loc[mask, 'date']) + MonthBegin(1)

    # 3. Update the 'col_n' value if needed
    df.loc[mask, 'raw_category'] = 'Salary'

    return df

def _normalize(val: str, case_sensitive: bool) -> str:
    """Strips and optionally lowercases a string."""
    val = str(val).strip()
    return val if case_sensitive else val.lower()


def _resolve_subcategory(raw_val: str, all_known: set, 
                         known_original_map: dict, manual_label: str, 
                         case_sensitive: bool,fuzzy_threshold: int
                         ) -> str:
    """
    Multi-tier matcher for a single raw value.

    Tier 1 — Exact match
    Tier 2 — Substring match  : known value is contained inside raw value  (e.g. "Jhangu Pharma" -> "Pharma")
    Tier 3 — Fuzzy match      : similarity score above threshold            (e.g. "Other" -> "Others")
    Tier 4 — No match         : returns manual_label
    """
    if pd.isna(raw_val):
        return manual_label

    normalized_raw = _normalize(raw_val, case_sensitive)

    # ── Tier 1: Exact match ──────────────────────────────────────────────────
    if normalized_raw in all_known:
        return known_original_map[normalized_raw]

    # ── Tier 2: Substring match (known token inside raw value) ───────────────
    # Prioritize longer matches to avoid short false positives (e.g. "Tea" inside "Tea_N_Others")
    substring_matches = [
        known_original_map[known]
        for known in all_known
        if known and known in normalized_raw
    ]
    if substring_matches:
        best = max(substring_matches, key=len)   # longest match wins
        logger.debug(f"Substring match: '{raw_val}' → '{best}'")
        return best

    # ── Tier 3: Fuzzy match ──────────────────────────────────────────────────
    result = process.extractOne(
        normalized_raw,
        all_known,
        scorer=fuzz.token_sort_ratio   # handles word-order variations too
    )
    if result and result[1] >= fuzzy_threshold:
        matched_key = result[0]
        best = known_original_map[matched_key]
        logger.debug(f"Fuzzy match ({result[1]}%): '{raw_val}' → '{best}'")
        return best

    # ── Tier 4: No match ────────────────────────────────────────────────────
    return manual_label


def create_subcategory(df: pd.DataFrame, mapping_dict: dict, 
                       source_col: str, target_col: str = "SubCategory", 
                       manual_label: str = "MANUAL", 
                       case_sensitive: bool = False,fuzzy_threshold: int = 80           
                       ) -> pd.DataFrame:
    """
    Validates raw category values against all known subcategories using
    multi-tier matching: exact → substring → fuzzy → MANUAL.

    Args:
        df              : Input DataFrame
        mapping_dict    : Dictionary with {category: [subcategory, ...]} structure
        source_col      : Column containing raw category values
        target_col      : Output column name (default: "SubCategory")
        manual_label    : Label for unresolved values (default: "MANUAL")
        case_sensitive  : Whether matching is case-sensitive (default: False)
        fuzzy_threshold : Minimum similarity score (0–100) for fuzzy match (default: 80)

    Returns:
        DataFrame with new SubCategory column added
    """
    # Build normalized lookup: normalized_value -> original_value
    known_original_map = {
        _normalize(sub, case_sensitive): sub
        for subs in mapping_dict.values()
        for sub in subs
    }
    all_known = set(known_original_map.keys())

    df = df.copy()
    df[target_col] = df[source_col].apply(
        lambda val: _resolve_subcategory(
            val, all_known, known_original_map, manual_label, case_sensitive, fuzzy_threshold
        )
    )

    # ── Audit log ─────────────────────────────────────────────────────────────
    manual_vals = df.loc[df[target_col] == manual_label, source_col].unique()
    if len(manual_vals) > 0:
        logger.warning(f"{len(manual_vals)} value(s) flagged as '{manual_label}': {list(manual_vals)}")
    else:
        logger.info(f"All values in '{source_col}' resolved successfully.")

    return df


def create_category(df: pd.DataFrame, mapping_dict: dict, 
                    source_col: str, target_col: str = "Category", 
                    unspecified_label: str = "Unspecified",case_sensitive: bool = False
                    ) -> pd.DataFrame:
    """
    Maps subcategory values to their parent category using the mapping dictionary.
    Unrecognized subcategories (including MANUAL) are labeled as Unspecified.
    """
    reverse_map = {
        _normalize(sub, case_sensitive): cat
        for cat, subs in mapping_dict.items()
        for sub in subs
    }

    df = df.copy()
    df[target_col] = df[source_col].apply(
        lambda val: reverse_map.get(_normalize(val, case_sensitive), unspecified_label)
        if not pd.isna(val) else unspecified_label
    )

    unspecified_vals = df.loc[df[target_col] == unspecified_label, source_col].unique()
    if len(unspecified_vals) > 0:
        logger.warning(f"{len(unspecified_vals)} value(s) mapped to '{unspecified_label}': {list(unspecified_vals)}")
    else:
        logger.info(f"All values in '{source_col}' mapped to a category successfully.")

    return df


def build_category_pipeline(df: pd.DataFrame, mapping_dict : dict = CATEGORY_FRM_SUBCATEGORY, raw_col: str = "raw_category", 
                            subcategory_col: str = "SubCategory", 
                            category_col: str = "Category",
                            fuzzy_threshold: int = 80) -> pd.DataFrame:
    """Runs create_subcategory → create_category in sequence."""
    df = create_subcategory(df, mapping_dict, source_col=raw_col, target_col=subcategory_col, fuzzy_threshold=fuzzy_threshold)
    df = create_category(df, mapping_dict, source_col=subcategory_col, target_col=category_col)
    return df