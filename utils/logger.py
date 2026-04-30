import logging
import os
import uuid
from datetime import datetime
from pathlib import Path


# ── Generated ONCE when logger.py is first imported ──────────────────────────
# All subsequent get_logger() calls across all modules reuse this same file.
_run_id       = os.environ.get("GITHUB_RUN_ID", str(uuid.uuid4())[:8])
_now          = datetime.now()
_month        = _now.strftime("%Y-%m")
_timestamp    = _now.strftime("%Y-%m-%d_%H-%M-%S")
_log_filename = f"{_month}_{_run_id}_{_timestamp}.log"
_log_dir      = Path("logs")
_log_dir.mkdir(exist_ok=True)
_log_path     = _log_dir / _log_filename


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that writes to both terminal and a single shared log file.

    All modules in one run share the same log file — generated once at import time.

    Log file naming: YYYY-MM_runID_timestamp.log
    Log line format: 2026-04-24 07:30:00 | src.ingestion.gdrive_reader | INFO | message

    Args:
        name (str): Always pass __name__ — shows exact module path in log lines.

    Returns:
        logging.Logger: Configured logger with console + file handlers.

    Usage:
        from utils.logger import get_logger
        logger = get_logger(__name__)

        logger.info("Process started")
        logger.warning("Unexpected value found")
        logger.error("Something went wrong")
        logger.critical("Pipeline failed entirely")
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured — reuse without adding duplicate handlers
        return logger

    logger.setLevel(logging.INFO)

    # ── Formatter ─────────────────────────────────────────────────────────────
    # Output: 2026-04-24 07:30:00 | src.ingestion.gdrive_reader | INFO | message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ── Console handler ───────────────────────────────────────────────────────
    # Prints to terminal locally and shows in GitHub Actions logs in CI.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ── File handler ──────────────────────────────────────────────────────────
    # All modules write to the same _log_path for the entire run.
    file_handler = logging.FileHandler(_log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Prevent bubbling to root logger — avoids duplicate lines.
    logger.propagate = False

    return logger