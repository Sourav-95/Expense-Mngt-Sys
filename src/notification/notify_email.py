from dotenv import load_dotenv
load_dotenv()

import os
import smtplib
from datetime import datetime
from email.message import EmailMessage

from utils.logger import get_logger

logger = get_logger(__name__)

# ── Load from .env / GitHub Secrets ──────────────────────────────────────────

GMAIL_FROM         = os.environ.get("GMAIL_FROM") or ""
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD") or ""
NOTIFY_TO_EMAIL    = os.environ.get("NOTIFY_TO_EMAIL") or ""

def validate_config():
    """
    Validates that all required notification env vars are set.
    Raises ValueError if any are missing.
    """
    missing = [
        var for var, val in {
            "GMAIL_FROM"        : GMAIL_FROM,
            "GMAIL_APP_PASSWORD": GMAIL_APP_PASSWORD,
            "NOTIFY_TO_EMAIL"   : NOTIFY_TO_EMAIL
        }.items() if not val
    ]
    if missing:
        raise ValueError(f"Missing notification config in .env: {missing}")
    
def build_subject(status: str, pipeline: str) -> str:
    """
    Builds email subject line.

    e.g. [EXP-PIPELINE-NOTIFICATION]| SUCCESS — 2026-04-29
    """
    # icon = "✅" if status == "SUCCESS" else "❌"
    date = datetime.now().strftime("%Y-%m-%d")
    return f"[EXP-PIPELINE-NOTIFICATION] | {pipeline} | {status} — {date}"

def build_body(status: str, pipeline: str, error=None, details=None) -> str:
    """
    Builds plain text email body.

    Args:
        status   : SUCCESS or FAILURE
        pipeline : DAILY or CONSOLIDATION
        error    : exception message if failure
        details  : optional extra info e.g. rows processed
    """
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    body = (
        f"Pipeline   : {pipeline}\n"
        f"Status     : {status}\n"
        f"Run Time   : {run_time}\n"
    )

    if details:
        body += f"Details    : {details}\n"

    if error:
        body += (
            f"\n── Error Occurred ───────────────────\n"
            f"{error}\n"
            f"───────────────────────────────────────\n"
        )

    body += "\n— expense-pipeline automated notification"
    return body

def notify(status: str, pipeline: str, error= None, details = None):
    """
    Sends an email notification for pipeline SUCCESS or FAILURE.

    Args:
        status   (str) : 'SUCCESS' or 'FAILURE'
        pipeline (str) : 'DAILY' or 'CONSOLIDATION'
        error    (str) : exception message — pass on failure
        details  (str) : optional run summary e.g. '209 rows processed'

    Usage:
        from src.notification.notify import notify

        # success
        notify(status="SUCCESS", pipeline="DAILY", details="209 rows processed")

        # failure
        notify(status="FAILURE", pipeline="DAILY", error=str(e))
    """
    try:
        validate_config()

        msg            = EmailMessage()
        msg["Subject"] = build_subject(status, pipeline)
        msg["From"]    = GMAIL_FROM
        msg["To"]      = NOTIFY_TO_EMAIL
        msg.set_content(build_body(status, pipeline, error, details))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_FROM, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)

        logger.info(f"Email Notification sent: {pipeline} | {status}")

    except Exception as e:
        logger.error(f"Failed to send Email notification: {e}")
    