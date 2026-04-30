from datetime import datetime
import pandas as pd

from utils.drive_utils import (
    get_or_create_folder,
    get_or_create_gsheet,
    read_gsheet_as_df,
    append_rows_to_gsheet
)
from utils.logger import get_logger
from config.constants import DEST_FOLDER_ID

logger = get_logger(__name__)


def anti_join(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns only rows in new_df whose uuid
    does not exist in existing_df.
    """
    if existing_df.empty:
        logger.info("Existing sheet empty — all rows treated as new")
        return new_df

    existing_keys = set(existing_df["uuid"].astype(str))
    delta_df      = new_df[~new_df["uuid"].astype(str).isin(existing_keys)]

    logger.info(f"Incoming rows : {len(new_df)}")
    logger.info(f"Existing rows : {len(existing_df)}")
    logger.info(f"Delta rows    : {len(delta_df)}")
    return delta_df


def _write_fresh(gsheet_auth, sheet_id: str, df: pd.DataFrame, month: str):
    """
    Writes entire dataframe into existing GSheet (sheet_id).
    Clears first then writes — no new file created.

    Args:
        gsheet_auth : authenticated gspread client
        sheet_id    : existing GSheet ID from get_or_create_gsheet()
        df          : dataframe to write
        month       : for logging only
    """
    ws = gsheet_auth.open_by_key(sheet_id).sheet1
    ws.clear()
    # ws.update([df.columns.tolist()] + df.astype(str).values.tolist()) --- IGNORE : Changed as caused error---
    # ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist()). --- IGNORE : Changed as caused error---
    # ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist(), value_input_option='RAW') --- IGNORE : Changed as caused error---
    ws.update( [df.columns.tolist()] + df.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED" )
    logger.info(f"Fresh write complete for month: {month} — {len(df)} rows written")


def _route_month(drive_auth, gsheet_auth, month: str, month_df: pd.DataFrame):
    """
    Handles routing for a single month group.

    - Gets or creates YYYY-MM folder
    - Gets or creates GSheet inside it
    - Fresh write if GSheet is empty, else anti-join + append

    Args:
        drive_auth  : authenticated GDrive service
        gsheet_auth : authenticated gspread client
        month       : e.g. '2026-04' — derived from data date column
        month_df    : rows belonging to this month
    """
    logger.info(f"Routing month: {month} — {len(month_df)} rows")

    # ── Step 1: get or create YYYY-MM folder ──────────────────────────────────
    month_folder_id = get_or_create_folder(drive_auth, DEST_FOLDER_ID, month)

    # ── Step 2: get or create GSheet inside folder ────────────────────────────
    sheet_name = f"{month}_raw"
    sheet_id   = get_or_create_gsheet(drive_auth, month_folder_id, sheet_name)

    # ── Step 3: read existing data ────────────────────────────────────────────
    existing_df = read_gsheet_as_df(gsheet_auth, sheet_id)

    if existing_df.empty:
        # ── Fresh write into existing sheet_id — no new file created ──────────
        logger.info(f"Fresh write for month: {month}")
        _write_fresh(gsheet_auth, sheet_id, month_df, month)

    else:
        # ── Incremental append ────────────────────────────────────────────────
        logger.info(f"Incremental append for month: {month}")
        delta_df = anti_join(month_df, existing_df)

        if delta_df.empty:
            logger.info(f"No new rows for month: {month} — skipping")
            return

        append_rows_to_gsheet(gsheet_auth, sheet_id, delta_df)


def route(drive_auth, gsheet_auth, df: pd.DataFrame):
    """
    Entry point for Layer 3 — Date Decision.

    Groups transformed df by month from date column — not datetime.now().
    Each month routed independently — handles all 3 conditions automatically.

    Condition 1: day == 1, data spans Apr + May
                 → Apr folder exists → append | May folder missing → fresh write
    Condition 2: day == 2, only May data
                 → May folder exists → anti-join → append delta
    Condition 3: vacation gap, data spans Apr + May
                 → same as Condition 1 — handled automatically

    Args:
        drive_auth  : authenticated GDrive service from get_drive_service()
        gsheet_auth : authenticated gspread client from get_sheets_service()
        df          : fully transformed dataframe with `date` and `uuid` columns
    """
    logger.info("Date router started")

    # ── derive month from data, not system date ───────────────────────────────
    df["months"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
    months       = sorted(df["months"].unique())
    logger.info(f"Months detected in data: {months}")

    for month in months:
        month_df = df[df["months"] == month].drop(columns=["months"]).copy()
        _route_month(drive_auth, gsheet_auth, month, month_df)

    logger.info("Date Loading / Incremental Load completed")