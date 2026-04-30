import pandas as pd
from src.transformation.transformer_ops import apply_raw_category_transformation, \
                            capitalize_raw_category, \
                            clean_raw_category, \
                            modify_salaried_record, \
                            data_preprocessor, \
                            build_category_pipeline
from src.transformation.unique_id_build import build_unique_key
from utils.logger import get_logger
logger = get_logger(__name__)


def transform_pipeline(data:pd.DataFrame, bank:str) -> pd.DataFrame:
    try:
        df = data.copy()
        df = data_preprocessor(df, bank)
        df = clean_raw_category(df)
        df = apply_raw_category_transformation(df)
        df = capitalize_raw_category(df)
        df = modify_salaried_record(df)
        df = build_category_pipeline(df)
        df = build_unique_key(df)
        try:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce').dt.strftime("%Y-%m-%d")
        except Exception as e:
            logger.error(f"Error converting 'date' column to datetime for bank `{bank}`: {e}", exc_info=True)
        logger.info(f"Final Columns after transformation: {df.columns.tolist()}")
        logger.info(f"Transformation pipeline completed for bank `{bank}`")
    except Exception as e:
        logger.error(f"An error occurred in the transformation pipeline for bank `{bank}`: {e}", exc_info=True)
    return df









