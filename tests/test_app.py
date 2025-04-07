import pytest
from unittest.mock import patch
from typing import List
from src.app import run_etl_pipeline


@pytest.fixture
def valid_file_paths() -> List[str]:
    """
    Provides a list of valid mock CSV file paths for testing the ETL pipeline.

    Returns:
        List[str]: A list of mock file path strings.
    """
    return ["file1.csv", "file2.csv"]


@pytest.fixture
def empty_file_paths() -> List[str]:
    """
    Provides an empty list of file paths to simulate no files being selected.

    Returns:
        List[str]: An empty list.
    """
    return []


@patch("src.app.extract_data")
@patch("src.app.transform_data")
@patch("src.app.load_data")
def test_pipeline_successful(
    mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths: List[str]
) -> None:
    """
    Test that the ETL pipeline completes successfully when all stages execute correctly.

    Args:
        mock_load_data: Mocked load_data function.
        mock_transform_data: Mocked transform_data function.
        mock_extract_data: Mocked extract_data function.
        valid_file_paths (List[str]): List of valid CSV file paths.

    Asserts:
        True is returned and all pipeline stages are called appropriately.
    """
    mock_extract_data.side_effect = [
        [{"customer_name": "John", "product": "Coffee", "price": "2.50"}],
        [{"customer_name": "Jane", "product": "Tea", "price": "1.75"}],
    ]
    mock_transform_data.return_value = {
        "final_transactions": [{"id": 1, "price": "2.50"}],
        "branch_data": {"branches_table": [], "transactions_with_branch_id": []},
        "product_data": {
            "products_table": [],
            "transactions_with_product_id": [],
            "transaction_product_table": [],
        },
    }

    result: bool = run_etl_pipeline(valid_file_paths)

    assert result is True
    mock_extract_data.assert_called()
    mock_transform_data.assert_called_once()
    mock_load_data.assert_called_once()


@patch("src.app.extract_data")
@patch("src.app.transform_data")
@patch("src.app.load_data")
def test_pipeline_no_extracted_data(
    mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths: List[str]
) -> None:
    """
    Test that the ETL pipeline exits gracefully with no data when nothing is extracted.

    Args:
        mock_load_data: Mocked load_data function.
        mock_transform_data: Mocked transform_data function.
        mock_extract_data: Mocked extract_data function.
        valid_file_paths (List[str]): List of CSV file paths.

    Asserts:
        False is returned and no transformation or load is attempted.
    """
    mock_extract_data.side_effect = [[], []]

    result: bool = run_etl_pipeline(valid_file_paths)

    assert result is False
    mock_extract_data.assert_called()
    mock_transform_data.assert_not_called()
    mock_load_data.assert_not_called()


@patch("src.app.extract_data")
@patch("src.app.transform_data")
@patch("src.app.load_data")
def test_pipeline_transformation_failure(
    mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths: List[str]
) -> None:
    """
    Test that the ETL pipeline raises an exception when the transformation step fails.

    Args:
        mock_load_data: Mocked load_data function.
        mock_transform_data: Mocked transform_data function.
        mock_extract_data: Mocked extract_data function.
        valid_file_paths (List[str]): List of CSV file paths.

    Asserts:
        An exception is raised with the expected message, and load is not called.
    """
    mock_extract_data.side_effect = [
        [{"customer_name": "John", "product": "Coffee", "price": "2.50"}],
        [{"customer_name": "Jane", "product": "Tea", "price": "1.75"}],
    ]
    mock_transform_data.side_effect = Exception("Transformation failed!")

    with pytest.raises(Exception, match="Transformation failed!"):
        run_etl_pipeline(valid_file_paths)

    mock_extract_data.assert_called()
    mock_transform_data.assert_called_once()
    mock_load_data.assert_not_called()


@patch("src.app.extract_data")
@patch("src.app.transform_data")
@patch("src.app.load_data")
def test_pipeline_loading_failure(
    mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths: List[str]
) -> None:
    """
    Test that the ETL pipeline raises an exception if the loading step fails.

    Args:
        mock_load_data: Mocked load_data function.
        mock_transform_data: Mocked transform_data function.
        mock_extract_data: Mocked extract_data function.
        valid_file_paths (List[str]): List of CSV file paths.

    Asserts:
        An exception is raised with the expected message, and all prior steps succeed.
    """
    mock_extract_data.side_effect = [
        [{"customer_name": "John", "product": "Coffee", "price": "2.50"}],
        [{"customer_name": "Jane", "product": "Tea", "price": "1.75"}],
    ]
    mock_transform_data.return_value = {
        "final_transactions": [{"id": 1, "price": "2.50"}],
        "branch_data": {"branches_table": [], "transactions_with_branch_id": []},
        "product_data": {
            "products_table": [],
            "transactions_with_product_id": [],
            "transaction_product_table": [],
        },
    }
    mock_load_data.side_effect = Exception("Loading failed!")

    with pytest.raises(Exception, match="Loading failed!"):
        run_etl_pipeline(valid_file_paths)


    mock_extract_data.assert_called()
    mock_transform_data.assert_called_once()
    mock_load_data.assert_called_once()
