import pytest
from io import StringIO
from src.extract.extract import extract_data

@pytest.fixture
def mock_config(mocker):
    """
    Mock the configuration loader to ensure consistent test behavior.
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
    mocker.patch("utils.config_loader.load_config", return_value=mock_config)
    return mock_config

def test_valid_csv_file(mock_config):
    """
    Test the function with a valid CSV file.
    """
    csv_data = StringIO("""Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
2023-04-05 12:00:00,Branch A,John Doe,Laptop,1200,Card,1234-5678-9123-4567
2023-04-05 13:00:00,Branch B,Jane Smith,Tablet,800,Cash,5678-9123-4567-1234""")
    
    result = extract_data(csv_data)
    assert len(result) == 2
    assert result[0] == {
        "customer_name": "John Doe",
        "product": "Laptop",
        "qty": "1",
        "price": "1200",
        "branch": "Branch A",
        "payment_type": "Card",
        "card_number": "1234-5678-9123-4567",
        "date_time": "2023-04-05 12:00:00"
    }

def test_missing_file(mock_config):
    """
    Test the function with a missing file path.
    """
    result = extract_data("non_existent_file.csv")
    assert result is None

def test_file_with_header_row(mock_config):
    """
    Test the function with a file that includes a header row.
    """
    csv_data = StringIO("""Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
2023-04-05 12:00:00,Branch A,John Doe,Laptop,1200,Card,1234-5678-9123-4567""")
    
    result = extract_data(csv_data)
    assert len(result) == 1
    assert result[0]["customer_name"] == "John Doe"

def test_insufficient_fields(mock_config):
    """
    Test the function with rows that have missing fields.
    """
    csv_data = StringIO("""Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
2023-04-05 12:00:00,Branch A,John Doe,Laptop,1200,,1234-5678-9123-4567""")
    
    result = extract_data(csv_data)
    assert len(result) == 0  # The row should be skipped

def test_empty_file(mock_config):
    """
    Test the function with an empty file.
    """
    csv_data = StringIO("")
    result = extract_data(csv_data)
    assert result == []

def test_file_with_duplicate_header_row(mock_config):
    csv_data = StringIO("""Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
Date/Time,Branch,Customer Name,Product,Price,Payment Type,Card Number
2023-04-05 12:00:00,Branch A,John Doe,Laptop,1200,Card,1234-5678-9123-4567""")
    result = extract_data(csv_data)
    assert len(result) == 1
    assert result[0]["customer_name"] == "John Doe"