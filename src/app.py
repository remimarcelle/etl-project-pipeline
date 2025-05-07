from dotenv import load_dotenv
load_dotenv("db/.env")
from typing import List, Dict, Any
import os
import logging
from utils.logger import get_logger
from src.extract.extract import extract_data
from src.transform.transform import transform_data
from db.db_cafe_alt_solution import load_data

logger = get_logger("app", log_level=logging.DEBUG)

def run_etl_pipeline(file_paths: List[str]) -> bool:
    """
    Executes the ETL pipeline for the provided CSV file paths or falls back to a default test dataset.
    The ETL pipeline consists of three main steps: Extract, Transform, and Load.

    The process involves:
        - Extracting data from one or more CSV files (or from 'data/raw-data.csv' if no files are provided).
        - Aggregating, normalising and transforming the data (removes PII, removes duplications,
        splits product details into separate columns)
        - Loading the final transformed data into a local MYSQL database.

    Args:
        file_paths (List[str]): List of CSV file paths to process.
            If empty or None, a default test file at 'data/raw-data.csv' is used.
    
    Returns:
         True if ETL completes successfully, otherwise raises an Exception usually
         due to no data or missing files.
        
    Raises:
        Exception: If any step of the ETL pipeline fails.
    """
    try:
        logger.info("Starting ETL pipeline...")
        
        # Beginning of the extraction process.
        using_test_file = False  # Track if we're in test mode
        if not file_paths:
            logger.warning("No file paths received. Falling back to default test dataset.")
            test_path = "data/raw-data.csv"
            if not os.path.exists(test_path):
                logger.error("Fallback file 'data/raw-data.csv' not found. ETL cannot proceed.")
                return False
            file_paths = [test_path]
            using_test_file = True
            logger.info(f"Using fallback test file: {test_path}")
        else:
            logger.info(f"Received {len(file_paths)} file(s) from GUI or CLI:")
            for path in file_paths:
                logger.info(f" → {path}")
        extracted_data: List[Dict[str, Any]] = []
        # Process all selected files.
        for file_path in file_paths:
            # extract_data expects a file path or file-like object
            logger.info(f"Extracting data from {file_path}...")
            data = extract_data(file_path)  
            if data:
                logger.info(f"Extracted {len(data)} records from {file_path}.")
                extracted_data.extend(data)
            else:
                logger.warning(f"No data extracted from {file_path}.")
            if not extracted_data:
                logger.warning("No data extracted from the selected files. ETL pipeline terminating gracefully.")
                return False

        # Transform the aggregated extracted data.
        logger.info(f"Transforming {len(extracted_data)} extracted records...")
        transformed_data = transform_data(extracted_data)
        if not transformed_data or not transformed_data.get("final_transactions"):
            logger.warning("Transformation produced no data. ETL pipeline terminating gracefully.")
            return False  # Graceful exit when transformation fails or produces no meaningful output
        logger.info(f"Transformation successful. Keys in transformed data: {list(transformed_data.keys())}")

        # Load data into the database using the consolidated load_data function.
        if using_test_file:
            logger.info("Test mode active. Skipping database upload.")
        else:
            logger.info("Loading transformed data into the database...")
            load_data(transformed_data)
            logger.info("Data loading completed successfully.")

        # If the pipeline completes without raising an exception, return True.
        logger.info("ETL pipeline completed successfully.")
        return True
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    import sys

    file_paths = sys.argv[1:]

    if not file_paths:
        print("No file paths provided. Exiting without running ETL.")
        sys.exit(0)

    try:
        success = run_etl_pipeline(file_paths)
        if success:
            print("ETL pipeline completed successfully.")
        else:
            print("ETL pipeline exited gracefully with no data.")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        sys.exit(1)




