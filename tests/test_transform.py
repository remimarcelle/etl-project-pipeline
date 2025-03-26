import os
import uuid
import pytest
from typing import Dict, List, Tuple, Any
from unittest.mock import patch
from utils.logger import get_logger
from src.transform.transform import (
    standardise_product_name,
    parse_product_field,
    filter_valid_records,
    normalise_branches,
    normalise_products,
    transform_data,
    write_normalised_csv_files
)

logger = get_logger("test_transform")

def test_standardise_product_name_strips_and_lowercases() -> None:
    """
    Tests that standardise_product_name strips whitespace and converts to lowercase.
    """
    original: str = "  Large Chai Latte  "
    expected: str = "large chai latte"

    result: str = standardise_product_name(original)

    assert result == expected, f"Expected '{expected}', got '{result}'"
    logger.info("standardise_product_name test passed.")

def test_parse_product_field_extracts_product_details() -> None:
    """
    Tests that parse_product_field correctly extracts product details
    into parsed_products.
    """
    record: Dict[str, str] = {
        "product": "Regular Latte - 2.50, Large Mocha - 2.70"
    }

    result: Dict[str, Any] = parse_product_field(record)

    assert "parsed_products" in result, "Missing 'parsed_products' key in result."
    assert isinstance(result["parsed_products"], list), "'parsed_products' should be a list."
    assert len(result["parsed_products"]) == 2, "Expected 2 parsed products."

    first_product = result["parsed_products"][0]
    expected_keys = ["size", "product_name", "flavour", "price"]
    for key in expected_keys:
        assert key in first_product, f"Missing key '{key}' in parsed product."

    logger.info("parse_product_field test passed.")

def test_filter_valid_records_excludes_incomplete_rows() -> None:
    """
    Tests that filter_valid_records removes rows with missing required fields.
    """
    valid_row: Dict[str, str] = {
        "customer_name": "Alice",
        "product": "Regular Latte - 2.50",
        "qty": "1",
        "price": "2.50",
        "branch": "London",
        "payment_type": "CARD",
        "date_time": "2024-01-01 10:00"
    }

    invalid_row: Dict[str, str] = {
        "customer_name": "Bob",
        "product": "",
        "qty": "1",
        "price": "3.00",
        "branch": "London",
        "payment_type": "CARD",
        "date_time": "2024-01-01 10:05"
    }

    input_data: List[Dict[str, str]] = [valid_row, invalid_row]

    result: List[Dict[str, str]] = filter_valid_records(input_data)

    assert len(result) == 1, f"Expected 1 valid record, got {len(result)}"
    assert result[0]["customer_name"] == "Alice"
    logger.info("filter_valid_records test passed.")

def test_normalise_branches_returns_expected_structure() -> None:
    """
    Tests that normalise_branches returns a branches_table and updated transactions.
    """
    transaction: Dict[str, str] = {
        "id": str(uuid.uuid4()),
        "branch": "Manchester"
    }

    transactions: List[Dict[str, str]] = [transaction]

    branches_table, updated_transactions = normalise_branches(transactions)

    assert isinstance(branches_table, list), "branches_table should be a list."
    assert isinstance(updated_transactions, list), "updated_transactions should be a list."
    assert len(branches_table) == 1, f"Expected 1 branch, got {len(branches_table)}"
    assert "branch_id" in updated_transactions[0], "Missing 'branch_id' in updated transaction."
    logger.info("normalise_branches test passed.")


def test_normalise_products_returns_expected_tables() -> None:
    """
    Tests that normalise_products returns products_table, updated transactions, and transaction_product_table.
    """
    transaction: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "parsed_products": [
            {"size": "large", "product_name": "mocha", "flavour": "", "price": 2.70},
            {"size": "regular", "product_name": "latte", "flavour": "", "price": 2.50}
        ]
    }

    transactions: List[Dict[str, Any]] = [transaction]

    products_table, updated_transactions, transaction_product_table = normalise_products(transactions)

    assert isinstance(products_table, list), "products_table should be a list."
    assert isinstance(updated_transactions, list), "updated_transactions should be a list."
    assert isinstance(transaction_product_table, list), "transaction_product_table should be a list."
    assert len(products_table) == 2, f"Expected 2 unique products, got {len(products_table)}"
    logger.info("normalise_products test passed.")

def test_transform_data_returns_expected_structure() -> None:
    """
    Tests that transform_data returns a dictionary with expected keys and structures.
    """
    sample_data: List[Dict[str, str]] = [
        {
            "customer_name": "Jesse Franco",
            "product": "Regular Flavoured iced latte - Hazelnut - 2.75, Large Speciality Tea - Green - 1.60",
            "qty": "1",
            "price": "4.35",
            "branch": "Edinburgh",
            "payment_type": "CASH",
            "card_number": "",
            "date_time": "21/04/2024 09:00"
        }
    ]

    result: Dict[str, Any] = transform_data(sample_data)

    assert isinstance(result, dict), f"Expected dict, got {type(result)}"

    # Top-level keys
    expected_keys: List[str] = ["final_transactions", "branch_data", "product_data"]
    for key in expected_keys:
        assert key in result, f"Missing key '{key}' in transformed data."

    # Branch data
    assert isinstance(result["branch_data"], dict)
    assert "branches_table" in result["branch_data"]
    assert "transactions_with_branch_id" in result["branch_data"]

    # Product data
    assert isinstance(result["product_data"], dict)
    assert "products_table" in result["product_data"]
    assert "transactions_with_product_id" in result["product_data"]
    assert "transaction_product_table" in result["product_data"]

    # Final transactions
    assert isinstance(result["final_transactions"], list)
    assert len(result["final_transactions"]) > 0
    logger.info("transform_data structure test passed.")

    logger.info("Cleaned transactions:")
    for record in result["final_transactions"]:
        logger.info(record)

    logger.info("Normalised products:")
    for product in result["product_data"]["products_table"]:
        logger.info(product)

    logger.info("Normalised products:")
    for product in result["product_data"]["products_table"]:
        logger.info(product)


@patch("src.transform.transform.write_csv")
def test_write_normalised_csv_files_with_mock(mock_write_csv) -> None:
    """
    Tests that write_normalised_csv_files calls write_csv correctly
    without writing real files.
    """
    sample_data: List[Dict[str, str]] = [
        {
            "customer_name": "Jesse Franco",
            "product": "Regular Flavoured iced latte - Hazelnut - 2.75, Large Speciality Tea - Green - 1.60",
            "qty": "1",
            "price": "4.35",
            "branch": "Edinburgh",
            "payment_type": "CASH",
            "card_number": "",
            "date_time": "21/04/2024 09:00"
        }
    ]

    result: Dict[str, Any] = transform_data(sample_data)

    final_transactions = result["final_transactions"]
    second_normalised = (
        result["branch_data"]["branches_table"],
        result["branch_data"]["transactions_with_branch_id"]
    )
    third_normalised = (
        result["product_data"]["products_table"],
        result["product_data"]["transactions_with_product_id"],
        result["product_data"]["transaction_product_table"]
    )

    write_normalised_csv_files(final_transactions, second_normalised, third_normalised)


    expected_file_count: int = 4
    actual_file_count: int = mock_write_csv.call_count
    assert actual_file_count == expected_file_count, f"Expected {expected_file_count} write calls, got {actual_file_count}"

    logger.info("write_normalised_csv_files mock test passed.")

