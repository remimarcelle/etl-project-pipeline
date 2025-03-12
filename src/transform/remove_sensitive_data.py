import logging
from typing import List, Dict
from utils.logger import get_logger
import copy

logger = get_logger("remove_sensitive_data", log_level=logging.DEBUG)

def remove_pii(data: List[Dict[str, str]], sensitive_fields: List[str] = None) -> List[Dict[str, str]]:
    """
    Removes specified sensitive fields (PII) (i.e., customer_name and card_number) from the provided dataset.

    This function iterates over the list of records (each record as a dict) and removes
    the keys 'customer_name' and 'card_number' if they exist.
    
    Args:
        data (List[Dict[str, str]]): List of raw data rows.
        sensitive_fields (List[str], optional): List of fields to remove sensitive information.
            Defaults to ['Customer Name', 'Card Number'].

    Returns:
        List[Dict[str, str]]: Data with sensitive fields removed.
    Raises:
        Exception: if any unexpected error occurs during processing.
    """
    try:
        # Use default fields if none are specified
        if sensitive_fields is None:
            sensitive_fields = ['Customer Name', 'Card Number']

        logger.info(f"Removing sensitive fields: {sensitive_fields}")

        # Make a deep copy of the data to avoid modifying the original data
        data_copy = copy.deepcopy(data)

        # Process each record and remove sensitive fields
        for record in data_copy:
            for field in sensitive_fields:
                if field in record:
                    del record[field]
                    logger.debug(f"Removed {field} from record.")

        logger.info(f"Successfully removed sensitive fields from {len(data)} records.")
        return data_copy
    except Exception as e:
        logger.error(f"Error while removing sensitive information: {e}")