import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from utils.logger import get_logger

from dotenv import load_dotenv
load_dotenv()

logger = get_logger(__name__)

# ── Scopes ────────────────────────────────────────────────────────────────────
# Drive  : read/write files and folders in Google Drive
# Sheets : read/write Google Sheets data
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def _get_credentials() -> service_account.Credentials:
    """
    Loads service account credentials from the JSON key file.

    The path is read from SA_KEY_PATH environment variable.
    - Locally    : set SA_KEY_PATH=sa_key.json in your .env or terminal
    - GitHub CI  : workflow writes the secret to /tmp/sa_key.json and sets
                   SA_KEY_PATH=/tmp/sa_key.json as an env var

    Raises:
        FileNotFoundError : if the key file path is missing or wrong
        ValueError        : if SA_KEY_PATH env var is not set
    """
    key_path = os.environ.get("SA_KEY_PATH")

    if not key_path:
        raise ValueError(
            "SA_KEY_PATH environment variable is not set. "
            "Set it to the path of your service account JSON key file."
        )

    if not os.path.exists(key_path):
        raise FileNotFoundError(
            f"Service account key file not found at: {key_path}"
        )

    logger.info(f"Loading credentials from: {key_path}")

    creds = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=SCOPES
    )

    logger.info("Credentials loaded successfully")
    return creds


def get_drive_service():
    """
    Builds and returns an authenticated Google Drive API client.

    Used for:
    - Listing files/folders in Drive
    - Downloading .xlsx input files
    - Creating monthly output folders
    - Uploading archived reports

    Returns:
        googleapiclient.discovery.Resource: Drive v3 service client
    """
    creds = _get_credentials()
    service = build("drive", "v3", credentials=creds)
    logger.info("Google Drive service initialised")
    return service


def get_sheets_service():
    """
    Builds and returns an authenticated Google Sheets API client (gspread).

    Used for:
    - Creating new GSheets inside monthly folders
    - Writing/appending data to tabs
    - Reading existing sheet data for incremental loads

    Returns:
        gspread.Client: Authorised gspread client
    """
    creds = _get_credentials()
    client = gspread.authorize(creds)
    logger.info("Google Sheets service initialised")
    return client