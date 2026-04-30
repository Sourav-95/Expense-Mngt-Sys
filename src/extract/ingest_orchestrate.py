from utils.drive_utils import download_file, list_files_from_gdrive
import pandas as pd
from io import BytesIO
from src.extract.extract import read_file_from_gdrive, data_cleaner
from utils.logger import get_logger

logger = get_logger(__name__)

def source_reader(service_auth, source_folder_id: str, folder_mime_type: str, max_file_no:int) -> pd.DataFrame:
    """
    Reads files from the source GDrive folder using the utility function `list_files_from_gdrive` from `utils/drive_utils.py`. 
    This function is called by `main.py` (subject to change) and serves as the entry point for the extraction step.
    Args:
    - source_folder_id (str): GDrive folder ID from `main.py`
    - folder_mime_type (str): from `main.py`
    """
    # gdrive_ops_obj = GDriveOps(service = service_auth)    # Initialize the GDriveOps object (service will be set in __init__)

    try:
        list_of_files = list_files_from_gdrive(service_auth, source_folder_id, folder_mime_type, max_file_no)
    except Exception as e:
        logger.error(f"Error occurred while reading files from GDrive: {e}")
        list_of_files = []
    
    # Initialize an empty DataFrame to hold merged data
    merged_data = pd.DataFrame()  

    for file in list_of_files:
        try:
            file_content = download_file(service_auth, file['id'])
            logger.info(f"Content Size for `{file['name']}` :- {(file_content.getbuffer().nbytes)/1024} kb)")

            ## DO THE FILE INGESTION 
            ## OPERATION YOU WANT WITH THE FILE CONTENT HERE
            df = read_file_from_gdrive(file['name'], file_content)
            df = data_cleaner(df, file_name = file['name'])                                          # type: ignore since already handled in the function definition with default value
            merged_data = pd.concat([merged_data, df], ignore_index=True)  # Merge the cleaned data into the main DataFrame 

        except Exception as e:
            logger.error(f"Error occurred while downloading file `{file['name']}` (ID: {file['id']}): {e}")
    
    return merged_data