import pytest
from io import StringIO
from typing import List, Dict
from src.extract.extract import extract_data

@pytest.fixture
def mock_config(mocker) -> dict:
    """
    Mock the configuration loader to ensure consistent test behavior.

    Returns:
        dict: A configuration with default headers for CSV files and a default quantity.
    """
    mock_config = {
        "default_headers": [
            "Date/Time",
            "Branch",
            "Customer Name",
            "Product",
            "Price",
            "Payment Type",
            "Card Number"
        ],
        "default_qty": "1"
    }
    # Patch the config loader used by extract_data
    mocker.patch("utils.config_loader.load_config", return_value=mock_config)
    return mock_config

def test_valid_csv_file(mock_config: dict) -> None:
    """
    Test the function with a valid CSV file.

    The CSV contains a header row and two data rows. The header row is skipped,
    and the remaining rows are parsed into dictionaries with snake_case keys,
    with a default quantity of "1".

    Returns:
        None
    """
    csv_data = StringIO(
        "Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number\n"
        "2023-04-05 12:00:00,Branch A,John Doe,Laptop,1200,Card,1234-5678-9123-4567\n"
        "2023-04-05 13:00:00,Branch B,Jane Smith,Tablet,800,Cash,\n"
    )
    
    result: List[Dict[str, str]] = extract_data(csv_data)
    assert result is not None, "Expected a list of dictionaries but got None."
    assert len(result) == 2, f"Expected 2 records, got {len(result)}."
    
    expected_first: Dict[str, str] = {
        "customer_name": "John Doe",
        "product": "Laptop",
        "qty": "1",
        "price": "1200.00",  # assuming your formatting yields two decimals
        "branch": "Branch A",
        "payment_type": "Card",
        "card_number": "1234-5678-9123-4567",
        "date_time": "2023-04-05 12:00:00"
    }

    expected_second: Dict[str, str] = {
        "customer_name": "Jane Smith",
        "product": "Tablet",
        "qty": "1",
        "price": "800.00",
        "branch": "Branch B",
        "payment_type": "Cash",
        "card_number": "",  # For Cash payments, card number is empty.
        "date_time": "2023-04-05 13:00:00"
    }

    # Adjust assertions if the order of records might vary.
    assert result[0] == expected_first or result[1] == expected_first, f"Expected first record mismatch."
    assert result[0] == expected_second or result[1] == expected_second, f"Expected second record mismatch."



def test_missing_file(mock_config: dict) -> None:
    """
    Tests that extract_data returns None when the file path does not exist.
    
    Returns:
        None
    """
    result = extract_data("non_existent_file.csv")
    assert result is None

def test_file_with_header_row(mock_config: dict) -> None:
    """
    Tests extraction of data from a CSV file that includes a header row.
    
    The file includes one header row and one data row.
    The function should skip the header row and return one processed record.

    Returns:
        None
    """
    csv_data = StringIO(
        "Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number\n"
        "2023-04-05 12:00:00,Branch A,John Doe,Laptop,12.00,Card,1234-5678-9123-4567\n"
    )

    result: List[Dict[str, str]] = extract_data(csv_data)

    assert len(result) == 1, f"Expected 1 record, got {len(result)}."
    assert result[0]["customer_name"] == "John Doe", (
        f"Expected 'John Doe', got {result[0]['customer_name']}."
    )

def test_insufficient_fields(mock_config: dict) -> None:
    """
    Tests that extract_data skips rows that have missing fields.
    """
    csv_data = StringIO("""Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
2023-04-05 12:00:00,Branch A,John Doe,Laptop,12.00,,1234-5678-9123-4567""")
    
    result = extract_data(csv_data)
    assert len(result) == 0  

def test_empty_file(mock_config: dict) -> None:
    """
    Tests that extract_data returns an empty list for an empty CSV input.

    Returns:
        None
    """
    csv_data = StringIO("")
    result = extract_data(csv_data)
    assert result == []

def test_file_with_duplicate_header_row(mock_config: dict) -> None:
    """Tests that extract_data correctly handles CSV input with duplicate header rows.

    The file mistakenly includes two identical header rows at the top.
    The function should skip both header rows and only process the data row.

    Returns:
        None
    """
    csv_data = StringIO(
        "Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number\n"
        "Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number\n"
        "2023-04-05 12:00:00,Branch A,John Doe,Laptop,12.00,Card,1234-5678-9123-4567\n"
    )

    result: List[Dict[str, str]] = extract_data(csv_data)

    assert len(result) == 1, f"Expected 1 record after skipping duplicate headers, got {len(result)}."
    assert result[0]["customer_name"] == "John Doe", (
        f"Expected 'John Doe', got {result[0]['customer_name']}."
    )