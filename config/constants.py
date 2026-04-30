# Source:
MAX_FILE_NO = 2     # maximum number of files allowed in the source folder for processing
SOURCE_FOLDER_ID = "1yDjHhI__AD0Dm7DDGfliSYo9KbR7W_qW"                                  # Replace with your GDrive folder ID
DEST_FOLDER_ID   = "1aCyfcBOqHQ-G5L54KCHQbpeTm36cx6Hf"                                  # Replace with your GDrive folder ID

# mime types
XLSX_MIME   = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
XLS_MIME    = "application/vnd.ms-excel"                                                # Older .xls format
GSHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

# sheet tab names used in MasterData GSheet
TAB_SUMMARY = "Summary"
TAB_DETAIL  = "Detail"
TAB_PIVOT   = "Pivot"

# input file pattern (not used any more)
INPUT_FILE_PREFIX = "expense_report"

# Global Variables used across modules
BANK_VARIABLES = {
    "axis": {
        "skip_row_value" : 17,
        "split_particular" : '/'
    },
    "hdfc": {
        "skip_row_value" : 20,
        "split_particular" : ' '
    }
}

# Used in Ingestion Cleaning
EXPECTED_COLUMN = ['date', 'narration', 'withdrawal amt.', 'deposit amt.']
THRESHOLD_NULL_FOR_ROW = 2

# Category mapping for cleaning the 'raw_category' column in the transformation step
SUBCATEGORY_MAPPING = {
        'restau'        : 'Restaurant',
        'rehear'        : 'Rehearsal',
        'grocer'        : 'Grocery',
        'foods'         : 'Food',
        'food'          : 'Food',
        '0001transfer'  : 'Transfer',
        'transfer'      : 'Transfer',
        'others'        : 'Other',
        'other'         : 'Other',
        'paymen'        : 'Payment',
        'advanc'        : 'Advance',
        'coffee'        : 'Tea',
        'tea'           : 'Tea',
        'upi'           : 'upi',
        'puja'          : 'Puja',
        'maid'          : 'Maid',
        'fuel'          : 'Fuel',
        'rent'          : 'Rent',
        'home'          : 'HomeAccesories',
        'wife d'        : 'Wife dress',
        'wife b'        : 'Wife bird',
        'return'        : 'Return',
        'recurrin'      : 'Recurring',
        'pay fo'        : 'Payment',
        'sweet'         : 'Sweets',
        'sweets'        : 'Sweets',
        'fruits'        : 'Fruits',
        'fruit'         : 'Fruits',
        'juice'         : 'Juice'
    }

CATEGORY_FRM_SUBCATEGORY = {
    "Loan"          :   ["HDFC", "Fibe", "Credit Card", "Money View"],
    "Rent"          :   ["Home", "Subscriptions","Rent"],
    "Bill"          :   ["Mobile", "Internet", "Water", "Electricity", "Maid", "Netflix", "Gadget", "Recharge", "Googleplay"],
    "HomeAccesories":   ["Daily Usage", "HomeAccesories", "Ekart"],
    "Personal"      :   ["Hair", "Personal", "Rehearsal", "Music", "Entertainment", "Cloths", "Father", "Tour", "Unseen", "Donate", "Gift"],
    "Medical"       :   ["Wife", "Self", "Parents", "Pharma"],
    "Restuarants"   :   ["DineIn", "Online Order", "Restuarants", "hsr street", "Swiggy", "Zomato"],
    "Wife"          :   ["Medical", "Recurring", "Birds", "Extras", "Cloths", "Future Exp", "Wife dress", "Wife bird"],
    "Grocery"       :   ["Meat", "Grocery", "Fruits", "Juice"],
    "Transportation":   ["Fuel", "Service", "Wash & Maintain", "AutoCab"],
    "Tea_N_Others"  :   ["Tea", "Others"],
    "Food"          :   ["Snacks", "Regular", "Office Food", "Cooking Gas", "Food", "HungerBox", "Sweets"],
    "Puja"          :   ["Flowers", "One Time", "Kalibari", "Puja"],
    "Investment"    :   ["Angel One", "Zerodha"],
    "Transfer"      :   ["Transfer"],
    "Income"        :   ["Salary", "Interest", "Dividend"]
}

# Unique key builder used for incremental load in loader.py
KEY_COLUMNS  = ["date", "particulars", "dr", "cr"]
MIN_TID_LEN  = 5
INVALID_TIDS = {"FOR", "TO", "BY", "ON", "AT", "THE", "AND", "FROM", "VIA"}

# Consolidation
DASHBOARD_FOLDER_ID = "1EG6dzy2uKExg6D8xDqjz0Ddoa4nsclOo"
CONSOLIDATED_SHEET_NAME = "consolidated_master"
API_SLEEP_SECONDS       = 2