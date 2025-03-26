from uuid import uuid4
import csv
import os
import logging
from typing import List, Dict, Any, Tuple
from utils.logger import get_logger
from utils.config_loader import load_config
from src.transform.remove_sensitive_data import remove_pii
from src.transform.remove_duplicates import deduplicate_data

logger = get_logger("transform", log_level=logging.DEBUG)

config = load_config()  # Load the default config.json file

# Recognised sizes (only volume sizes)
known_sizes = config['known_sizes']

def standardise_product_name(name: str) -> str:
    """
    Standardises the product name by converting it to lower case and stripping whitespace.
    
    Args:
        name (str): The original product name.
        
    Returns:
        str: The normalised product name.
    """
    return name.lower().strip()

def parse_product_field(record: Dict[str, str]) -> Dict[str, Any]:
    """
    Parses the 'product' field to extract product details - size, product_name, flavour and price.
    
    Supports several formats:
      - Simple entry, e.g. "iced latte - 2.50".  
        (Here "iced latte" is treated as a single product name, since "iced" is not in known_sizes.)
      - Detailed entry, e.g. "Large Latte - Hazelnut - 2.45".   
        If the first token (after splitting by space) is in known_sizes, it is taken as the size;
        the next token(s) form the product name; any additional token (if present) is taken as flavour.
      - Multiple entries separated by commas, e.g.  
        "Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45".  
        Each entry is split and parsed. Prices are aggregated.
    
    The parsed result is stored under a new key "parsed_products" (a list of dicts), and
    the record's overall "price" field is updated to the sum of all parsed prices.
    
    Args:
        record (Dict[str, str]): A transaction record with a "product" field.
        
    Returns:
        Dict[str, Any]: The updated record with parsed product details.
    """
    try:
        product_field = record.get("product", "").strip()
        if not product_field:
            return record

        # If multiple entries are present, split by commas; otherwise create a list with one entry.
        entries = [entry.strip() for entry in product_field.split(",")] if "," in product_field else [product_field]
        parsed_products = []
        total_price: float = 0.0

        for entry in entries:
            parts = entry.split(" - ", maxsplit=2)
            if len(parts) == 2:
                # Simple format: "Product Name - Price"
                product_name = standardise_product_name(parts[0])
                size = ""
                flavour = ""
                try:
                    price_val: float = float(parts[1].strip())
                except ValueError:
                    logger.warning(f"Invalid price '{parts[1]}' in entry '{entry}'.")
                    continue
            elif len(parts) >= 3:
                tokens = parts[0].split()
                if tokens and tokens[0].lower() in known_sizes:
                    size = tokens[0].lower()
                    product_name = standardise_product_name(" ".join(tokens[1:]))
                else:
                    size = ""
                    product_name = standardise_product_name(parts[0])
                if len(parts) == 3:
                    flavour = parts[1].strip()
                else:
                    flavour = " ".join(parts[1:-1]).strip()
                try:
                    price_val = float(parts[-1].strip())
                except ValueError:
                    logger.warning(f"Invalid price '{parts[-1]}' in entry '{entry}'.")
                    continue
            else:
                logger.warning(f"Skipping product entry '{entry}' due to unexpected format.")
                continue

            parsed_products.append({
                "size": size,
                "product_name": product_name,
                "flavour": flavour,
                "price": price_val
            })
            total_price += price_val

        if parsed_products:
            record["parsed_products"] = parsed_products
            record["price"] = f"{total_price:.2f}"
        return record
    except Exception as e:
        logger.error(f"Error parsing product field in record {record}: {e}")
        return record

def filter_valid_records(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Filters out records missing essential fields.
    
    Essential fields (in snake_case) are:
      customer_name, product, qty, price, branch, payment_type, date_time.
    
    Args:
        data (List[Dict[str, str]]): Extracted transaction records.
        
    Returns:
        List[Dict[str, str]]: Only records with non-empty values for all required fields.
    """
    required = config["required_fields"]
    valid = []
    for record in data:
        if all(record.get(field, "").strip() for field in required):
            valid.append(record)
        else:
            logger.warning(f"Skipping record due to missing fields: {record}")
    return valid

def normalise_branches(data: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Normalises branch data by extracting unique branch names and assigning each a GUID.
    
    Replaces the 'branch' key in each transaction with a 'branch_id' that references the branch table.
    
    Args:
        data (List[Dict[str, str]]): Transaction records containing the key "branch".
        
    Returns:
        Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
            - branches_table: A list of unique branch records.
            - updated_transactions: Transactions updated with "branch_id" instead of "branch".
    """
    branches: List[Dict[str, str]] = []
    updated_transactions: List[Dict[str, str]] = []
    branch_ids: Dict[str, str] = {}

    for record in data:
        branch = record.get("branch", "").strip()
        if not branch:
            logger.warning(f"Record missing branch info: {record}")
            continue
        if branch not in branch_ids:
            branch_id: str = str(uuid4())
            branch_ids[branch] = branch_id
            branches.append({"id": branch_id, "name": branch})
        new_record = record.copy()
        new_record.pop("branch", None)
        new_record["branch_id"] = branch_ids[branch]
        updated_transactions.append(new_record)
    logger.info(f"Normalised branches: {len(branch_ids)} unique branches found.")
    return branches, updated_transactions

def normalise_products(transactions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, str]], List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Normalises product data by extracting unique product records and creating a transaction_product mapping.
    
    For each transaction that contains a "parsed_products" key, the function:
      - Constructs a products_table with unique records, each with keys: "id", "product_name", "size", "flavour", "price".
      - Creates a transaction_product mapping linking each transaction (by its "id") to its product record(s) via a unique product_id.
      - Updates the transaction by removing the "product" and "parsed_products" fields and adding a "product_id" key 
        (a comma-separated string of the product IDs).
    
    Args:
        transactions (List[Dict[str, Any]]): Transaction records containing the "parsed_products" field.
        
    Returns:
        Tuple[List[Dict[str, str]], List[Dict[str, Any]], List[Dict[str, str]]]:
            - products_table: A list of unique product records.
            - updated_transactions: Transactions updated with "product_id".
            - transaction_product_table: Records mapping transaction_id to product_id.
    """
    products_table: List[Dict[str, str]] = []
    updated_transactions: List[Dict[str, Any]] = []
    transaction_product_table: List[Dict[str, str]] = []
    product_ids: Dict[tuple, str] = {}

    for record in transactions:
        parsed_products = record.get("parsed_products")
        if not parsed_products:
            updated_transactions.append(record)
            continue

        current_product_ids = []
        for entry in parsed_products:
            key = (
                entry.get("product_name", "").lower(),
                entry.get("size", "").lower(),
                entry.get("flavour", "").lower()
            )
            if key not in product_ids:
                product_id = str(uuid4())
                product_ids[key] = product_id
                products_table.append({
                    "id": product_id,
                    "product_name": entry.get("product_name", ""),
                    "size": entry.get("size", ""),
                    "flavour": entry.get("flavour", ""),
                    "price": str(entry.get("price", "0"))
                })
            current_product_ids.append(product_ids[key])
            transaction_product_table.append({
                "id": str(uuid4()),
                "transaction_id": record.get("id", ""),
                "product_id": product_ids[key]
            })
        new_record = record.copy()
        new_record.pop("product", None)
        new_record.pop("parsed_products", None)
        new_record["product_id"] = ", ".join(current_product_ids)
        updated_transactions.append(new_record)

    logger.info(f"Normalised products: found {len(products_table)} unique product records.")
    return products_table, updated_transactions, transaction_product_table

def transform_data(data: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Processes and normalises transaction data into a structured dictionary containing:
    
    - final_transactions: Final processed transactions ready for loading.
    - branch_data: A dictionary containing:
        - branches_table: Unique branch records.
        - transactions_with_branch_id: Transactions updated with a branch_id.
    - product_data: A dictionary containing:
        - products_table: Unique product records.
        - transactions_with_product_id: Transactions updated with product_id.
        - transaction_product_table: A mapping of transaction_id to product_id.
    
    Args:
        data (List[Dict[str, str]]): Extracted transaction records.
    
    Returns:
        Dict[str, Any]: A dictionary with keys "final_transactions", "branch_data", and "product_data".
    
    Raises:
        Exception: If transaction processing fails.
    """
    try:
        logger.info("Starting transaction processing...")

        # step 1. Remove sensitive information.
        cleaned_records = remove_pii(data)
        logger.info(f"Sensitive information removed from {len(data)} records.")
        
        # step 2. Filter out records missing required fields.
        valid_records = [r for r in cleaned_records if all(r.get(k, "").strip() for k in [
    "product", "qty", "price", "branch", "payment_type", "date_time"
])]
        logger.info(f"{len(valid_records)} records remain after filtering for required fields.")

        if not valid_records:
            logger.warning("No valid records found after filtering. Exiting transformation early.")
            return {
                "final_transactions": [],
                "branch_data": {
                    "branches_table": [],
                    "transactions_with_branch_id": []
        },
        "product_data": {
            "products_table": [],
            "transactions_with_product_id": [],
            "transaction_product_table": []
        }
    }
        
        # step 3. Parse the 'product' field.
        parsed_records = [parse_product_field(record) for record in valid_records]
        if parsed_records:
            logger.debug(f"First parsed record: {parsed_records[0]}")
        logger.info("Product field parsing complete.")
        
        # Remove duplicate records.
        first_normalised_form = deduplicate_data(parsed_records)
        logger.info(f"Duplication eradictor process complete: {len(first_normalised_form)} unique records remain.")
        
        # Normalise branch data.
        branches_table, transactions_with_branch_id = normalise_branches(first_normalised_form)
        # second_normalised_form = (branches_table, transactions_with_branch_id)
        
        # Normalise product data and produce a transaction_product mapping table.
        products_table, transactions_with_product_id, transaction_product_table = normalise_products(transactions_with_branch_id)
        # third_normalised_form = (products_table, transactions_with_product_id, transaction_product_table)
        
        # Final transactions (fourth normalised form) are those after product normalisation.
        final_transactions = transactions_with_product_id
        
        logger.info("Transaction processing and normalisation complete.")
        
        # Construct a self-documenting return structure.
        return {
            "final_transactions": final_transactions,
            "branch_data": {
                "branches_table": branches_table,
                "transactions_with_branch_id": transactions_with_branch_id,
            },
            "product_data": {
                "products_table": products_table,
                "transactions_with_product_id": transactions_with_product_id,
                "transaction_product_table": transaction_product_table,
            }
        }
    except Exception as e:
        logger.error(f"Failed to process transactions: {e}")
        return {
            "final_transactions": [],
            "branch_data": {
                "branches_table": [],
                "transactions_with_branch_id": []
            },
            "product_data": {
                "products_table": [],
                "transactions_with_product_id": [],
                "transaction_product_table": []
            }
        }

def write_csv(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Writes a list of dictionaries to a CSV file.
    
    Args:
        data (List[Dict[str, Any]]): The data to be written.
        filename (str): The target filename (including path).
    """
    if not data:
        logger.warning(f"No data available to write to {filename}.")
        return
    fieldnames = list(data[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"CSV file written: {filename}")

def write_normalised_csv_files(
    final_transactions: List[Dict[str, Any]],
    second_normalised: Tuple[List[Dict[str, str]], List[Dict[str, str]]],
    third_normalised: Tuple[List[Dict[str, str]], List[Dict[str, Any]], List[Dict[str, str]]]) -> None:
    """
    Writes the normalised data into separate CSV files.
    
    Output CSV files are created for:
      - Transactions (final_transactions)
      - Branches (from second_normalised)
      - Products (from third_normalised)
      - Transaction_product mapping (from third_normalised)
    
    Filenames are generated dynamically using the branch name from the first branch record.
    
    Args:
        final_transactions (List[Dict[str, Any]]): Final transaction records.
        second_normalised (Tuple[List[Dict[str, str]], List[Dict[str, str]]]): Tuple (branches_table, transactions with branch_id).
        third_normalised (Tuple[List[Dict[str, str]], List[Dict[str, Any]], List[Dict[str, str]]]): Tuple (products_table, transactions with product_id, transaction_product_table).
    """
    branches_table, _ = second_normalised
    if not branches_table:
        logger.error("No branch data available for filename generation.")
        return
    branch_name = branches_table[0]["name"].replace(" ", "_")
    filenames = {
        "transaction": f"transaction-{branch_name}.csv",
        "branch": f"branch-{branch_name}.csv",
        "product": f"product-{branch_name}.csv",
        "transaction_product": f"transaction_product-{branch_name}.csv"
    }

    # Ensure the output directory exists.
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    write_csv(final_transactions, os.path.join("output", filenames["transaction"]))
    write_csv(branches_table, os.path.join("output", filenames["branch"]))
    products_table, _, transaction_product_table = third_normalised
    write_csv(products_table, os.path.join("output", filenames["product"]))
    write_csv(transaction_product_table, os.path.join("output", filenames["transaction_product"]))
    logger.info("All normalised CSV files have been written.")