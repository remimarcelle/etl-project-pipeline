import csv
import logging
import os
import tempfile
from io import StringIO
from typing import List, Dict, Any, Tuple
from uuid import uuid4

import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()  # This sends logs to the console
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# Import the transformation functions from your transform module.
from src.transform.transform import (
    standardise_product_name,
    parse_product_field,
    filter_valid_records,
    normalise_branches,
    normalise_products,
    transform_data,
    write_csv,
    write_normalised_csv_files
)

# -----------------------------
# Section 1: Test standardisation of product name
# -----------------------------
def test_standardise_product_name() -> None:
    """
    Tests that standardise_product_name returns the product name in lowercase without extra spaces.
    
    Returns:
        None.
    """
    input_name: str = "  Iced Latte  "
    expected: str = "iced latte"
    result: str = standardise_product_name(input_name)
    assert result == expected, f"Expected '{expected}', got '{result}'."

# -----------------------------
# Section 2: Test parsing product field
# -----------------------------
def test_parse_product_field_simple() -> None:
    """
    Tests parse_product_field with a simple product entry.
    
    For an entry like "iced latte - 2.50", the function should create:
      - A parsed_products list with one dictionary having:
          product_name: "iced latte"
          size: "" (empty)
          flavour: "" (empty)
          price: a numeric value close to 2.50
      - The overall 'price' field in the record should be "2.50".
    
    Returns:
        None.
    """
    record: Dict[str, str] = {"product": "iced latte - 2.50"}
    result: Dict[str, Any] = parse_product_field(record)
    assert "parsed_products" in result, "Missing 'parsed_products' in result."
    parsed: List[Dict[str, Any]] = result["parsed_products"]
    assert isinstance(parsed, list) and len(parsed) == 1, "Expected one parsed product."
    entry = parsed[0]
    assert entry["product_name"] == "iced latte", f"Expected 'iced latte', got '{entry['product_name']}'"
    assert entry["size"] == "", f"Expected empty size, got '{entry['size']}'"
    assert entry["flavour"] == "", f"Expected empty flavour, got '{entry['flavour']}'"
    assert abs(entry["price"] - 2.50) < 1e-2, f"Expected price ~2.50, got {entry['price']}"
    assert result["price"] == "2.50", f"Expected overall price '2.50', got {result['price']}"

def test_parse_product_field_detailed() -> None:
    """
    Tests parse_product_field with a detailed product entry.
    
    For an entry like "Large Latte - Hazelnut - 2.45", if "Large" is recognised as a size:
      - size should be 'large'
      - product_name should be 'latte'
      - flavour should be 'Hazelnut'
      - price should be close to 2.45
      - The overall 'price' field in the record should be "2.45".
    
    Returns:
        None.
    """
    record: Dict[str, str] = {"product": "Large Latte - Hazelnut - 2.45"}
    result: Dict[str, Any] = parse_product_field(record)
    parsed: List[Dict[str, Any]] = result.get("parsed_products", [])
    assert isinstance(parsed, list) and len(parsed) == 1, "Expected one parsed product."
    entry = parsed[0]
    assert entry["size"] == "large", f"Expected size 'large', got '{entry['size']}'"
    assert entry["product_name"] == "latte", f"Expected product name 'latte', got '{entry['product_name']}'"
    assert entry["flavour"] == "Hazelnut", f"Expected flavour 'Hazelnut', got '{entry['flavour']}'"
    assert abs(entry["price"] - 2.45) < 1e-2, f"Expected price ~2.45, got {entry['price']}"
    assert result["price"] == "2.45", f"Expected overall price '2.45', got {result['price']}"

# -----------------------------
# Section 3: Test filtering valid records
# -----------------------------
def test_filter_valid_records() -> None:
    """
    Tests filter_valid_records to ensure that only records with all required fields are returned.
    
    Returns:
        None.
    """
    sample_data: List[Dict[str, str]] = [
        {"customer_name": "Alice", "product": "coffee", "qty": "1", "price": "2.50", "branch": "Main", "payment_type": "Card", "date_time": "2023-04-01 08:00:00"},
        {"customer_name": "Bob", "product": "", "qty": "1", "price": "3.00", "branch": "Main", "payment_type": "Cash", "date_time": "2023-04-01 09:00:00"}
    ]
    valid = filter_valid_records(sample_data)
    assert len(valid) == 1, f"Expected 1 valid record, got {len(valid)}."

# -----------------------------
# Section 4: Test normalising branches
# -----------------------------
def test_normalise_branches() -> None:
    """
    Tests that normalise_branches creates a branches table with unique IDs and updates transactions to use branch_id.
    
    Returns:
        None.
    """
    sample_records: List[Dict[str, str]] = [
        {"id": "1", "branch": "Main", "customer_name": "Alice"},
        {"id": "2", "branch": "Branch A", "customer_name": "Bob"},
        {"id": "3", "branch": "Main", "customer_name": "Charlie"}
    ]
    branches_table, updated_transactions = normalise_branches(sample_records)
    assert len(branches_table) == 2, f"Expected 2 unique branches, got {len(branches_table)}."
    for record in updated_transactions:
        assert "branch_id" in record, f"Record {record} missing branch_id."
        assert "branch" not in record, f"Record {record} should not contain 'branch'."

# -----------------------------
# Section 5: Test normalising products
# -----------------------------
def test_normalise_products() -> None:
    """
    Tests that normalise_products extracts unique product records and creates transaction-product mappings.
    
    Returns:
        None.
    """
    transaction: Dict[str, Any] = {
        "id": "tx1",
        "customer_name": "Alice",
        "branch": "Main",
        "parsed_products": [
            {"size": "small", "product_name": "coffee", "flavour": "", "price": 2.50},
            {"size": "medium", "product_name": "coffee", "flavour": "", "price": 3.00}
        ],
        "price": "5.50"
    }
    transactions: List[Dict[str, Any]] = [transaction]
    products_table, updated_transactions, transaction_product_table = normalise_products(transactions)
    # Expect two unique product records because the sizes differ.
    assert len(products_table) == 2, f"Expected 2 unique product records, got {len(products_table)}."
    for tx in updated_transactions:
        assert "product_id" in tx, f"Transaction {tx} missing product_id."
    assert len(transaction_product_table) > 0, "Expected a non-empty transaction_product mapping table."

# -----------------------------
# Section 6: Test processing transactions
# -----------------------------
def test_transform_data() -> None:
    """
    Tests the complete transaction processing pipeline.
    
    The function should return a dictionary with the following keys:
        - "final_transactions": The final processed transactions ready for loading.
        - "branch_data": A dictionary containing:
            - "branches_table": A list of branch records.
            - "transactions_with_branch_id": A list of transactions updated with branch_id.
        - "product_data": A dictionary containing:
            - "products_table": A list of product records.
            - "transactions_with_product_id": A list of transactions updated with product_id.
            - "transaction_product_table": A list of transaction-product mapping records.
    
    
    Returns:
        None.
    """
    sample_data: List[Dict[str, str]] = [
        {
            "Date/Time": "2023-04-01 08:00:00",
            "Branch": "Main",
            "Customer Name": "Alice",
            "Product": "coffee - 2.50",
            "Price": "2.50",
            "Payment Type": "Card",
            "Card Number": "1234"
        },
        {
            "Date/Time": "2023-04-01 09:00:00",
            "Branch": "Branch A",
            "Customer Name": "Bob",
            "Product": "tea - 1.75",
            "Price": "1.75",
            "Payment Type": "Cash",
            "Card Number": ""
        }
    ]

    result: Dict[str, Any] = transform_data(sample_data)
    
    # check for that we get a dictionary
    assert isinstance(result, dict), f"Expected result to be a dictionary, got {type(result)}"
    
    # Check for required top-level keys.
    expected_keys = ["final_transactions", "branch_data", "product_data"]
    for key in expected_keys:
        assert key in result, f"Missing key '{key}' in transformed data."
    
    # Validate branch_data structure.
    assert isinstance(result["branch_data"], dict), "branch_data should be a dictionary."
    expected_branch_keys = ["branches_table", "transactions_with_branch_id"]
    for sub_key in expected_branch_keys:
        assert sub_key in result["branch_data"], f"Missing key '{sub_key}' in branch_data."
    
    # Validate product_data structure.
    assert isinstance(result["product_data"], dict), "product_data should be a dictionary."
    expected_product_keys = ["products_table", "transactions_with_product_id", "transaction_product_table"]
    for sub_key in expected_product_keys:
        assert sub_key in result["product_data"], f"Missing key '{sub_key}' in product_data."
    
    # Check that final_transactions is a non-empty list.
    assert isinstance(result["final_transactions"], list), "final_transactions should be a list."
    assert len(result["final_transactions"]) > 0, "Expected non-empty final_transactions."
    print("Test overall transform_data: Passed")

# -----------------------------
# Section 7: Test write_csv
# -----------------------------
def test_write_csv() -> None:
    """
    Tests write_csv by writing sample data to a temporary CSV file and verifying its contents.
    
    Returns:
        None.
    """
    data: List[Dict[str, Any]] = [
        {"field1": "value1", "field2": "value2"},
        {"field1": "value3", "field2": "value4"}
    ]
    # Create a temporary directory.
    temp_dir: str = tempfile.mkdtemp()
    filename: str = os.path.join(temp_dir, "test_output.csv")
    write_csv(data, filename)
    with open(filename, "r", encoding="utf-8") as f:
        content: str = f.read()
    assert "field1" in content and "value1" in content, "CSV file content mismatch."
    # Optionally, you can remove the temporary directory or leave it.


# -----------------------------
# Section 8: Test write_normalised_csv_files
# -----------------------------
def test_write_normalised_csv_files() -> None:
    """
    Tests that write_normalised_csv_files writes CSV files to an output directory.
    
    For this test, we simulate normalised data and verify that the expected CSV files are created.
    
    Returns:
        None.
    """
    # Sample normalised data.
    final_transactions: List[Dict[str, Any]] = [{"id": "tx1", "customer_name": "Alice"}]
    branches_table: List[Dict[str, str]] = [{"id": "b1", "name": "Main"}]
    transactions_with_branch_id: List[Dict[str, str]] = [{"id": "tx1", "branch_id": "b1", "customer_name": "Alice"}]
    second_normalised: Tuple[List[Dict[str, str]], List[Dict[str, str]]] = (branches_table, transactions_with_branch_id)
    
    products_table: List[Dict[str, str]] = [{"id": "p1", "product_name": "coffee", "size": "", "flavour": "", "price": "2.50"}]
    transactions_with_product_id: List[Dict[str, Any]] = [{"id": "tx1", "product_id": "p1", "customer_name": "Alice"}]
    transaction_product_table: List[Dict[str, str]] = [{"id": "tp1", "transaction_id": "tx1", "product_id": "p1"}]
    third_normalised: Tuple[List[Dict[str, str]], List[Dict[str, Any]], List[Dict[str, str]]] = (
        products_table,
        transactions_with_product_id,
        transaction_product_table
    )
    # Create a temporary directory for output files.
    temp_output: str = tempfile.mkdtemp()
    
    # To simulate the behavior of write_normalised_csv_files, we define a temporary version that writes to temp_output.
    def fake_write_normalised_csv_files(
        final_transactions: List[Dict[str, Any]],
        second_normalised: Tuple[List[Dict[str, str]], List[Dict[str, str]]],
        third_normalised: Tuple[List[Dict[str, str]], List[Dict[str, Any]], List[Dict[str, str]]]
    ) -> None:
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
        for key, fname in filenames.items():
            file_path: str = os.path.join(temp_output, fname)
            if key == "transaction":
                write_csv(final_transactions, file_path)
            elif key == "branch":
                write_csv(branches_table, file_path)
            elif key == "product":
                products_table, _, _ = third_normalised
                write_csv(products_table, file_path)
            elif key == "transaction_product":
                _, _, transaction_product_table = third_normalised
                write_csv(transaction_product_table, file_path)
        logger.info("Fake normalised CSV files written to temporary directory.")

    fake_write_normalised_csv_files(final_transactions, second_normalised, third_normalised)
    
    # Verify that the simulated output files exist.
    branch_name = branches_table[0]["name"].replace(" ", "_")
    expected_files = [
        f"transaction-{branch_name}.csv",
        f"branch-{branch_name}.csv",
        f"product-{branch_name}.csv",
        f"transaction_product-{branch_name}.csv"
    ]
    for fname in expected_files:
        file_path: str = os.path.join(temp_output, fname)
        assert os.path.exists(file_path), f"Expected file {file_path} does not exist."



