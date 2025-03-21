from dotenv import load_dotenv
load_dotenv()
from typing import List, Dict, Any
import logging
from utils.logger import get_logger
from src.extract.extract import extract_data
from src.transform.transform import transform_data
from db.db_cafe_alt_solution import load_data

logger = get_logger("app", log_level=logging.DEBUG)

def run_etl_pipeline(file_paths: List[str]) -> bool:
    """
    Executes the ETL pipeline for the provided CSV file paths.

    The process involves:
        - Extracting data from each CSV.
        - Aggregating and transforming the data.
        - Loading the final data into the database.

    Args:
        file_paths (List[str]): A list of CSV file paths selected by the user.
    
    Returns:
         True if ETL completes successfully, otherwise raises an Exception.
        
    Raises:
        Exception: If any step of the ETL pipeline fails.
    """
    try:
        logger.info("Starting ETL pipeline...")
        
        # Beginning of the extraction process.
        logger.info(f"Processing {len(file_paths)} file(s) for extraction...")
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
        logger.info("Loading transformed data into the database...")
        load_data(transformed_data)
        logger.info("Data loading completed successfully.")

        # If the pipeline completes without raising an exception, return True.
        logger.info("ETL pipeline completed successfully.")
        return True
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise



