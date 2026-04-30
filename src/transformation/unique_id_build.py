import hashlib
import pandas as pd
from config.constants import KEY_COLUMNS
from utils.logger import get_logger

logger = get_logger(__name__)




def build_unique_key(df: pd.DataFrame, unique_key = 'uuid') -> pd.DataFrame:
    """
    Adds a `unique_key` column by hashing first 4 source columns:
    date + particulars + dr + cr

    - Nulls replaced with empty string
    - Values lowercased and stripped before hashing
    - Deterministic — same row always produces same key
    """
    logger.info("Building unique keys...")

    missing = [col for col in KEY_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    def _hash_row(row: pd.Series) -> str:
        concat = "|".join(
            str(row[col]).strip().lower() if pd.notna(row[col]) else ""
            for col in KEY_COLUMNS
        )
        return hashlib.sha256(concat.encode("utf-8")).hexdigest()[:16]

    df[unique_key] = df.apply(_hash_row, axis=1)

    logger.info(f"Total rows processed for `uuid`: {len(df)}")

    return df