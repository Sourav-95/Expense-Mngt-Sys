from config.constants import SOURCE_FOLDER_ID, \
        DEST_FOLDER_ID, XLS_MIME, GSHEET_MIME, \
        MAX_FILE_NO
import pandas as pd
from src.extract.ingest_orchestrate import source_reader
from src.transformation.transform_orchestrate import transform_pipeline
from src.load.data_router import route as data_router_route
from utils.logger import get_logger
from utils.auth import get_drive_service, get_sheets_service
from src.notification.notify_email import notify

logger = get_logger(__name__)

def main():
    logger.info("Starting main function")

    drive_auth = get_drive_service()              # Get the authenticated GDrive service
    sheet_auth = get_sheets_service()            # Get the authenticated GSheet service

    try:
        # Step 1: Data Ingestion
        data_ingested = source_reader(drive_auth, SOURCE_FOLDER_ID, XLS_MIME, MAX_FILE_NO)
        logger.info(f"Total Data ingested from GDrive: {data_ingested.shape[0]} rows, {data_ingested.shape[1]} columns")
        
        # Step 2: Preprocessing for Transformation--Group by bank and apply transform_pipeline for each bank
        unique_banks = data_ingested['bank'].unique()
        transformed_dfs = []

        # Step 3: Data Transformation
        for bank in unique_banks:
            bank_data = data_ingested[data_ingested['bank'] == bank].copy()
            logger.info(f"Transforming data for bank: {bank}")
            transformed_bank = transform_pipeline(bank_data, bank)
            transformed_dfs.append(transformed_bank)

        # Step 4: Combine all transformed data
        data_transformed = pd.concat(transformed_dfs, ignore_index=True)

        # Step 5: Load transformed data back to GDrive & GSheet
        data_router_route(drive_auth, sheet_auth, data_transformed)

        # Step 6: Send success notification
        notify(status="SUCCESS", 
               pipeline="ETL", 
               details=f"Processed {data_ingested.shape[0]} rows from {len(unique_banks)} banks."
               )
    except Exception as e:
        logger.error(f"Error in main pipeline: {e}")
        notify(status="FAILURE", pipeline="ETL", error=str(e))
        return

if __name__ == "__main__":
    main()