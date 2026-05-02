from dotenv import load_dotenv
load_dotenv()

import time
import pandas as pd
import numpy as np
from utils.auth import get_drive_service, get_sheets_service
from utils.drive_utils import (list_files_from_gdrive,
                               read_gsheet_as_df,
                               get_or_create_folder,
                               get_or_create_gsheet
                               )
from utils.logger import get_logger
from src.notification.notify_email import notify
from config.constants import (FOLDER_MIME,
                              GSHEET_MIME,
                              DEST_FOLDER_ID,
                              DASHBOARD_FOLDER_ID,
                              CONSOLIDATED_SHEET_NAME,
                              API_SLEEP_SECONDS
                              )

logger = get_logger(__name__)


def _list_month_folders(drive_auth) -> list[dict]:
    """
    Lists all YYYY-MM subfolders inside OUTPUT/DEST folder.
    Returns list of folder metadata dicts with id and name.
    """
    folders = list_files_from_gdrive(
        service_auth = drive_auth,
        folder_id    = DEST_FOLDER_ID,
        mime_type    = FOLDER_MIME,
        max_file_no  = 999          # no limit — all month folders
    )
    logger.info(f"Found {len(folders)} month folders in output")
    return folders


def _find_gsheet_in_folder(drive_auth, folder_id: str, folder_name: str) -> str | None:
    """
    Finds the GSheet inside a given YYYY-MM folder.
    Returns sheet_id if found, None if folder is empty.

    Args:
        folder_id   : YYYY-MM folder ID
        folder_name : for logging e.g. '2026-04'
    """
    sheets = list_files_from_gdrive(
        service_auth = drive_auth,
        folder_id    = folder_id,
        mime_type    = GSHEET_MIME,
        max_file_no  = 999
    )

    if not sheets:
        logger.warning(f"No GSheet found in folder: {folder_name} — skipping")
        return None

    sheet_id = sheets[0]["id"]
    logger.info(f"Found GSheet in {folder_name}: {sheet_id}")
    return sheet_id


def _read_all_monthly_sheets(drive_auth, gsheet_auth) -> pd.DataFrame:
    """
    Iterates all YYYY-MM folders, reads each GSheet into a dataframe,
    tags each row with source month, and concatenates into master_df.

    Sleeps between reads to avoid GSheet API rate limits.

    Returns:
        pd.DataFrame: combined master dataframe across all months
    """
    month_folders = _list_month_folders(drive_auth)

    if not month_folders:
        raise ValueError("No month folders found in output — nothing to consolidate")

    all_dfs = []

    for folder in month_folders:
        folder_id   = folder["id"]
        folder_name = folder["name"]

        sheet_id = _find_gsheet_in_folder(drive_auth, folder_id, folder_name)
        if not sheet_id:
            continue

        logger.info(f"Reading GSheet for month: {folder_name}")
        df = read_gsheet_as_df(gsheet_auth, sheet_id)

        if df.empty:
            logger.warning(f"GSheet is empty for month: {folder_name} — skipping")
            continue

        # tag each row with source month for dashboard filtering
        df["source_month"] = folder_name
        all_dfs.append(df)

        logger.info(f"Read {len(df)} rows from {folder_name}")

        # pause to avoid hitting GSheet API rate limits
        time.sleep(API_SLEEP_SECONDS)

    if not all_dfs:
        raise ValueError("All monthly sheets are empty — nothing to consolidate")

    return pd.concat(all_dfs, ignore_index=True)


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops duplicate rows based on uuid column.
    Handles edge case where same record appears in multiple monthly sheets.

    Returns:
        pd.DataFrame: deduplicated dataframe
    """
    before = len(df)
    df     = df.drop_duplicates(subset=["uuid"], keep="first")
    after  = len(df)

    if before != after:
        logger.warning(f"Duplicates removed: {before - after} rows dropped")
    else:
        logger.info("No duplicates found across monthly sheets")

    return df


def _write_consolidated(gsheet_auth, master_df: pd.DataFrame):
    """
    Writes master_df to consolidated_master GSheet
    inside the dashboard/ folder.

    Always fresh write — master is fully rebuilt every run.

    Args:
        gsheet_auth : authenticated gspread client
        master_df   : final consolidated dataframe
    """
    drive_auth = get_drive_service()

    # get or create dashboard folder
    dashboard_folder_id = get_or_create_folder(
        drive_auth,
        DASHBOARD_FOLDER_ID,
        "Dashboard"
    )

    # get or create consolidated GSheet
    sheet_id = get_or_create_gsheet(
        drive_auth,
        dashboard_folder_id,
        CONSOLIDATED_SHEET_NAME
    )

    # fresh write — clear and rewrite entire master
    ws = gsheet_auth.open_by_key(sheet_id).sheet1
    ws.clear()
    ws.update(
        [master_df.columns.tolist()] + master_df.fillna("").astype(str).values.tolist(),
        value_input_option="USER_ENTERED"
    )

    logger.info(f"Consolidated master written: {len(master_df)} rows → {CONSOLIDATED_SHEET_NAME}")

def add_filter_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds filter_app column for dashboard filtering.
    Income / Investment / Expense classification based on Category.
    """
    conditions = [
        df["Category"].isin(["Income"]),
        df["Category"].isin(["Investment", "Debt Clearance"]),
        ~df["Category"].isin(["Income", "Investment", "Debt Clearance", "Transfer"])
    ]
    values = ["Income", "Investment", "Expense"]

    df["filter_app"] = np.select(conditions, values, default="")
    return df

def run():
    """
    Entry point for consolidation pipeline.

    Steps:
    1. List all YYYY-MM folders in output
    2. Read GSheet from each folder → dataframe
    3. Tag each row with source month
    4. Concat all → master_df
    5. Deduplicate on uuid
    6. Sort by date ascending
    7. Fresh write to consolidated_master GSheet

    Called by:
    - consolidate_main.py (standalone)
    - GitHub Actions consolidate.yml (after daily_pipeline succeeds)
    """
    logger.info("Consolidation pipeline started")

    drive_auth  = get_drive_service()
    gsheet_auth = get_sheets_service()

    try:
        # ── Step 1-3: read all monthly sheets ─────────────────────────────────
        master_df = _read_all_monthly_sheets(drive_auth, gsheet_auth)

        # ── Step 4: deduplicate ────────────────────────────────────────────────
        master_df = _deduplicate(master_df)

        # ── Step 5: sort by date ascending ────────────────────────────────────
        master_df = master_df.sort_values("date", ascending=True).reset_index(drop=True)

        # Filter the required columns only
        master_df = add_filter_col(master_df)

        logger.info(f"Master dataset ready: {len(master_df)} rows across {master_df['source_month'].nunique()} months")

        # ── Step 6: write consolidated GSheet ─────────────────────────────────
        _write_consolidated(gsheet_auth, master_df)

        # ── Notify success ─────────────────────────────────────────────────────
        notify(
            status   = "Success",
            pipeline = "Semantic Layer",
            details  = f"{len(master_df)} rows consolidated across {master_df['source_month'].nunique()} months"
        )

    except Exception as e:
        logger.error(f"Consolidation pipeline failed: {e}")
        notify(
            status   = "Failure",
            pipeline = "Semantic Layer",
            error    = str(e)
        )
        raise


if __name__ == "__main__":
    run()