import csv
from io import StringIO
import os
from typing import List, Dict, Optional, Union
import logging
from utils.logger import get_logger
from utils.config_loader import load_config

logger = get_logger("extract", log_level=logging.DEBUG)  # Creates extract.log

# Load configuration
config = load_config()

def extract_data(file_input: Union[str, StringIO]) -> Optional[List[Dict[str, str]]]:
    """
    Extracts records from a CSV file (or file-like object) that lacks a proper header row,
    then reorders each row into a dictionary with snake_case keys.

    The assumed raw CSV order (per row) is:
        [Date/Time, Branch, Customer Name, Product, Price, Payment Type, Card Number]

    The function outputs records with keys:
        customer_name, product, qty, price, branch, payment_type, card_number, date_time

    Args:
        file_input (Union[str, StringIO]): Either a file path (str) or a file-like object containing the CSV data.

    Returns:
        Optional[List[Dict[str, str]]]: A list of dictionaries in the desired field order, or
                                         None if the file is not found.
    Raises:
        Exception: If an unexpected error occurs during extraction.
    """
    # Load defaults from the configuration file
    # Define the expected headers in the raw CSV file (as they appear in the file).
    # Assumed raw order per row: [Date/Time, Branch, Customer Name, Product, Price, Payment Type, Card Number]
    default_headers: List[str] = config['default_headers']

    # We want to output dictionaries with keys in snake_case:
    #   customer_name, product, qty, price, branch, payment_type, card_number, date_time

    # Default quantity
    default_qty: str = config['default_qty']

    try:
        # Obtain a file-like object.
        if isinstance(file_input, str):
            if not os.path.exists(file_input):
                logger.error(f"File not found: {file_input}")
                return None
            logger.info(f"Extracting data from {file_input}...")
            with open(file_input, mode="r", newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file, fieldnames=default_headers)
                raw_data: List[Dict[str, str]] = list(reader)

        elif isinstance(file_input, StringIO):
            logger.info("Extraction taking place")
            reader = csv.DictReader(csv_file, fieldnames=default_headers)
            raw_data: List[Dict[str, str]] = list(reader)
        else:
            logger.error("Invalid file input type. Expected a file path (str) or file-like object (StringIO).")
            return None

        # Optional: if the file accidentally contains a header row, skip it.
        if raw_data and raw_data[0].get("Date/Time", "").strip() == "Date/Time":
            logger.info("Detected header row in file; skipping it.")
            raw_data = raw_data[1:]

        parsed_data: List[Dict[str, str]] = []
        for row_number, row in enumerate(raw_data, start=1):
            # Check if the row contains all required fields; if not, log a warning and skip.
            if len(row) < len(default_headers):
                logger.warning(f"Row {row_number} skipped due to insufficient fields: {row} (expected {len(default_headers)} fields).")
                continue
            try: 
                # Reorder and rename keys using snake_case.
                mapped_row: Dict[str, str] = {
                    "customer_name": row["Customer Name"].strip(),
                    "product": row["Product"].strip(),
                    "qty": default_qty,  # Default quantity since not in raw data.
                    "price": row["Price"].strip(),
                    "branch": row["Branch"].strip(),
                    "payment_type": row["Payment Type"].strip(),
                    "card_number": row["Card Number"].strip(),
                    "date_time": row["Date/Time"].strip()
                }
                parsed_data.append(mapped_row)
            except KeyError as e:
                logger.warning(f"Row {row_number} skipped due to missing field: {e}")

        logger.info(f"Successfully extracted {len(parsed_data)} rows from {file_input}.")
        if parsed_data:
            logger.debug(f"Sample extracted row: {parsed_data[0]}")
        return parsed_data

    except FileNotFoundError:
        logger.error(f"The file {file_input} was not found. Please ensure it exists in the 'data/' directory.")
        raise
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise



 
  
