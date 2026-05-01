from dotenv import load_dotenv
load_dotenv()

from datetime import datetime

import pandas as pd
import os, sys
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

from utils.auth import get_drive_service
from utils.logger import get_logger

logger = get_logger(__name__)


def list_files_from_gdrive(service_auth, folder_id: str, mime_type: str, max_file_no:int) -> list[dict]:
    """
    Lists files inside a given GDrive folder.

    Args:
        folder_id (str) : GDrive folder ID to search in
        mime_type (str) : optional filter e.g.
                        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' for xlsx
                        'application/vnd.google-apps.folder' for folders
                        None returns all file types
                        

    Returns:
        list[dict]: list of file metadata dicts with keys: id, name, modifiedTime
    """
    service = service_auth

    query = f"'{folder_id}' in parents and trashed=false"
    if mime_type:
        query += f" and mimeType='{mime_type}'"

    results = service.files().list(
        q=query,
        orderBy="modifiedTime desc",
        fields="files(id, name, modifiedTime)"
    ).execute()

    files = results.get("files", [])
    if not files:
        logger.warning(f"No files found in folder: `{folder_id}`, terminating pipeline")
        files = []
    elif len(files) > max_file_no:
        logger.warning(f"""Found more than `{max_file_no}` file in folder: `{folder_id}`. 
                    Need only `{max_file_no}` latest file... Terminating pipeline to avoid ambiguity."""
                    )
        sys.exit(1)
    else:
        logger.info(f"Found {len(files)} file(s) in folder: `{folder_id}`")
    
    return files

def download_file(service_auth, file_id: str) -> io.BytesIO:
    """
    Downloads a file from GDrive into an in-memory buffer.
    No temp files written to disk — keeps the runner clean.

    Args:
        file_id (str): GDrive file ID to download

    Returns:
        io.BytesIO: file content as in-memory buffer
                    ready to pass directly into pandas.read_excel()
    """
    service = service_auth

    request = service.files().get_media(fileId=file_id)
    buffer  = io.BytesIO()

    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        progress, done = downloader.next_chunk()
        logger.debug(f"Download progress: {int(progress.progress() * 100)}%")

    buffer.seek(0)
    logger.info(f"File downloaded successfully for ID: {file_id}")
    return buffer

def upload_df_as_gsheet(service_auth, df: pd.DataFrame, parent_folder_id: str,
                        file_name: str,
                        gsheet_mime_type: str,
                        xlsx_mime_type: str) -> str:
    """
    Serializes a DataFrame as xlsx and uploads it as a native Google Sheet.

    Args:
        service_auth     : authenticated GDrive service
        df               : Input DataFrame to upload
        parent_folder_id : Destination folder ID in GDrive
        file_name        : Name of the GSheet e.g. '2026-04_raw'
        gsheet_mime_type : GSheet mime type from constants
        xlsx_mime_type   : xlsx mime type from constants

    Returns:
        str: ID of the uploaded Google Sheet
    """
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    file_metadata = {
        "name"    : file_name,
        "parents" : [parent_folder_id],
        "mimeType": gsheet_mime_type
    }
    media = MediaIoBaseUpload(
        buffer,
        mimetype=xlsx_mime_type,
        resumable=True
    )

    uploaded = service_auth.files().create(
        body       = file_metadata,
        media_body = media,
        fields     = "id"
    ).execute()

    logger.info(f"Uploaded GSheet: '{file_name}' → folder: {parent_folder_id}")
    return uploaded["id"]

def read_gsheet_as_df(service_auth, sheet_id: str) -> pd.DataFrame:
    """
    Reads existing GSheet (sheet1) into a dataframe.
    Used for anti-join before appending delta rows.

    Give service_auth as gspread client for Sheets API operations.

    Args:
        sheet_id (str): GSheet file ID

    Returns:
        pd.DataFrame: existing data or empty dataframe if sheet is blank
    """
    try:
        client = service_auth
        sh     = client.open_by_key(sheet_id)
        ws     = sh.sheet1
        data   = ws.get_all_records()

        if not data:
            logger.info(f"GSheet is empty: {sheet_id}")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        logger.info(f"Read existing GSheet: {sheet_id} — {len(df)} rows")
    except Exception as e:
        logger.error(f"Error reading GSheet: {sheet_id} — {str(e)}")
        return pd.DataFrame()
    return df

def get_or_create_folder(service_auth,parent_folder_id: str, folder_name: str) -> str:
    """
    Checks if a folder exists inside a parent folder.
    Creates it if it doesn't exist.

    Used to create YYYY-MM folders under outputs/ automatically.

    Give service_auth as `gdrive` client for Drive API operations.
    Args:
        parent_folder_id (str) : ID of the parent folder e.g. outputs/ folder ID
        folder_name      (str) : name of the subfolder e.g. '2026-04'

    Returns:
        str: folder ID (existing or newly created)
    """
    service = service_auth

    # check if folder already exists
    query = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{folder_name}' and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    existing = results.get("files", [])

    if existing:
        logger.info(f"Folder already exists: {folder_name} ({existing[0]['id']})")
        return existing[0]["id"]

    # create if not found
    folder_metadata = {
        "name"    : folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents" : [parent_folder_id]
    }
    folder = service.files().create(
        body=folder_metadata,
        fields="id"
    ).execute()

    logger.info(f"Created new folder: {folder_name} ({folder['id']})")
    return folder["id"]

def upload_file(
    parent_folder_id: str,
    file_name: str,
    buffer: io.BytesIO,
    mime_type: str
) -> str:
    """
    Uploads a file from an in-memory buffer to a GDrive folder.

    Args:
        parent_folder_id (str)      : destination folder ID in GDrive
        file_name        (str)      : name to give the uploaded file
        buffer           (BytesIO)  : file content as in-memory buffer
        mime_type        (str)      : e.g.
                                      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    Returns:
        str: ID of the uploaded file
    """
    service = get_drive_service()

    file_metadata = {
        "name"   : file_name,
        "parents": [parent_folder_id]
    }
    media = MediaIoBaseUpload(buffer, mimetype=mime_type)

    uploaded = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    logger.info(f"Uploaded file: {file_name} → folder: {parent_folder_id}")
    return uploaded["id"]

def create_gsheet(service_auth, parent_folder_id: str, sheet_name: str) -> str:
    """
    Creates a new blank Google Sheet inside a GDrive folder.

    Used by archive.py to create a fresh GSheet for each month
    e.g. report_2026-04 inside outputs/2026-04/

    Give service_auth as `gdrive` client for Sheets API operations.

    Args:
        parent_folder_id (str) : GDrive folder ID where sheet will be created
        sheet_name       (str) : name for the new GSheet e.g. 'report_2026-04'

    Returns:
        str: GSheet file ID (use this as GSHEET_ID for gspread operations)
    """
    service = service_auth

    file_metadata = {
        "name"    : sheet_name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents" : [parent_folder_id]
    }
    sheet = service.files().create(
        body=file_metadata,
        fields="id"
    ).execute()

    logger.info(f"Created GSheet: {sheet_name} ({sheet['id']}) in folder: {parent_folder_id}")
    return sheet["id"]

def get_or_create_gsheet(service_auth, parent_folder_id: str, sheet_name: str) -> str:
    """
    Checks if a GSheet with the given name exists in the folder.
    Returns its ID if found, creates a new one if not.

    Used on every daily run — first run of the month creates the sheet,
    subsequent runs reuse it.

    give service_auth as `gdrive` client for Sheets API operations.

    Args:
        parent_folder_id (str) : GDrive folder ID to check in
        sheet_name       (str) : name of the GSheet e.g. 'report_2026-04'

    Returns:
        str: GSheet file ID
    """
    service = service_auth

    query = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType='application/vnd.google-apps.spreadsheet' and "
        f"name='{sheet_name}' and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    existing = results.get("files", [])

    if existing:
        logger.info(f"GSheet already exists: {sheet_name} ({existing[0]['id']})")
        return existing[0]["id"]

    return create_gsheet(service, parent_folder_id, sheet_name)

def append_rows_to_gsheet(service_auth, sheet_id: str, df: pd.DataFrame):
    """
    Appends delta dataframe rows to the bottom of existing GSheet.
    Does not clear or overwrite — pure append only.

    Give service_auth as gspread client for Sheets API operations.

    Args:
        sheet_id (str)        : GSheet file ID resolved dynamically
        df       (pd.DataFrame): delta rows to append after anti-join
    """

    client = service_auth
    sh     = client.open_by_key(sheet_id)
    ws     = sh.sheet1

    ws.append_rows(df.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")
    logger.info(f"Appended {len(df)} rows to GSheet: {sheet_id}")

def move_files_to_processed(service_auth, input_folder_id: str, processed_folder_id: str, xls_mime_type) -> int:
    """
    Moves all files from input folder to processed/ subfolder
    after successful pipeline run.

    Service account can move files with Editor access even without ownership.

    Args:
        service_auth         : authenticated GDrive service
        input_folder_id      : SOURCE_FOLDER_ID — input/ folder
        processed_folder_id  : ID of processed/ subfolder inside input/

    Returns:
        int: total files moved
    """
    files = list_files_from_gdrive(
        service_auth = service_auth,
        folder_id    = input_folder_id,
        mime_type    = xls_mime_type,
        max_file_no  = 999
    )

    if not files:
        logger.info("No files to move — input folder empty")
        return 0

    moved_count = 0
    for file in files:
        service_auth.files().update(
            fileId        = file["id"],
            addParents    = processed_folder_id,
            removeParents = input_folder_id,
            fields        = "id, parents"
        ).execute()
        logger.info(f"Moved to processed: {file['name']} ({file['id']})")
        moved_count += 1

    logger.info(f"Total files moved to processed: {moved_count}")
    return moved_count