import logging
from typing import List, Dict
from utils.logger import get_logger

logger = get_logger("remove_duplicates", log_level=logging.DEBUG)

def deduplicate_data(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Removes duplicate records from data.

    Two records are considered duplicates if all key-value pairs match exactly.
    The function returns only unique records.

    Args:
        data (List[Dict[str, str]]): List of data rows.

    Returns:
        List[Dict[str, str]]: Unique data.
    Raises:
        Exception: If an unexpected error occurs during removal of duplicates.
    """
    seen: Set[Tuple[Tuple[str, str], ...]] = set()
    unique_data: List[Dict[str, Any]] = []

    try:
        logger.info("Starting removal of duplicate data...")
        for row in data:
            safe_row = {}
            for k, v in row.items():
                if isinstance(v, list):
                    # If it's a list of dicts, convert each dict to sorted tuple
                    if all(isinstance(item, dict) for item in v):
                        v = tuple(tuple(sorted(item.items())) for item in v)
                    else:
                        v = tuple(v)
                elif isinstance(v, dict):
                    v = tuple(sorted(v.items()))
                safe_row[k] = v

            unique_key = tuple(sorted(safe_row.items()))
            if unique_key not in seen:
                seen.add(unique_key)
                unique_data.append(row)
        logger.info(f"Duplicates removed. {len(unique_data)} unique rows remaining.")
        return unique_data
    except Exception as e:
        logger.error(f"Error during the removal of duplicates: {e}")
        raise
