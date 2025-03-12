from typing import List, Dict
from src.logger import get_logger

logger = get_logger("deduplication", log_level=20)

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
        Exception: If an unexpected error occurs during deduplication.
    """
    try:
        logger.info("Starting deduplication of data...")
        seen = set()
        unique_data = []
        for row in data:
            # Create a tuple of sorted key-value pairs. Sorting ensures the order doesn't affect equality.
            unique_key = tuple(row.items())
            if unique_key not in seen:
                seen.add(unique_key)
                unique_data.append(row)
        logger.info(f"Deduplication complete. {len(unique_data)} unique rows remaining.")
        return unique_data
    except Exception as e:
        logger.error(f"Error during deduplication: {e}")
        raise
