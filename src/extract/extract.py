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

def extract_data(file_input: Union[str, StringIO], has_header: bool = True) -> Optional[List[Dict[str, str]]]:
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
            reader = csv.DictReader(file_input, fieldnames=default_headers)
            raw_data: List[Dict[str, str]] = list(reader)
        else:
            logger.error("Invalid file input type. Expected a file path (str) or file-like object (StringIO).")
            return None

        # Optionally remove the header row if 'has_header' is True.
        if has_header and raw_data:
            # Normalize: strip spaces and convert values to lowercase for comparison.
            first_row_values = {value.strip().lower() for value in raw_data[0].values()}
            expected_values = {header.strip().lower() for header in default_headers}
            if first_row_values == expected_values:
                logger.info("Detected header row in file; skipping it.")
                raw_data = raw_data[1:]

        parsed_data: List[Dict[str, str]] = []
        for row_number, row in enumerate(raw_data, start=1):
            # Retrieve and normalize the payment type.
            try:
                payment_type: str = row["Payment Type"].strip()
            except KeyError:
                logger.warning(f"Row {row_number} skipped due to missing 'Payment Type': {row}")
                continue
                    
            # Define the list of required headers.
            # If payment type is cash, exclude "Card Number" from required fields.
            if payment_type.upper() == "CASH":
                required_fields = [header for header in default_headers if header != "Card Number"]
            else:
                required_fields = default_headers

            # Check that all required fields are present and non-empty.
            # If any required field is missing or empty, skip the row.
            missing_field = False
            for header in required_fields:
                if header not in row or not row[header].strip():
                    logger.warning(f"Row {row_number} skipped due to missing or empty field '{header}': {row}")
                    missing_field = True
                    break
            if missing_field:
                continue

            try:
                try:
                    # Convert the Price field to a float and then format it to two decimals.
                    price: str = f"{float(row['Price'].strip()):.2f}"
                except ValueError:
                    logger.warning(f"Row {row_number} skipped due to invalid price value: {row['Price']}")
                    continue

                # Determine the card number: if payment_type is cash, use an empty string.
                card_number: str = row["Card Number"].strip() if payment_type.upper() != "CASH" else ""

                # Reorder and rename keys using snake_case.
                mapped_row: Dict[str, str] = {
                    "customer_name": row["Customer Name"].strip(),
                    "product": row["Product"].strip(),
                    "qty": default_qty,  # Default quantity since not in raw data.
                    "price": price,
                    "branch": row["Branch"].strip(),
                    "payment_type": payment_type,
                    "card_number": card_number,
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



 
  
