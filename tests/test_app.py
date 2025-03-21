import pytest
from unittest.mock import patch, MagicMock
from src.app import run_etl_pipeline


@pytest.fixture
def valid_file_paths():
    """
    Provides a list of valid file paths for testing.
    """
    return ["file1.csv", "file2.csv"]


@pytest.fixture
def empty_file_paths():
    """
    Provides an empty list to simulate no files selected.
    """
    return []


@patch("app.extract_data")
@patch("app.transform_data")
@patch("app.load_data")
def test_pipeline_successful(mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths):
    """
    Test the ETL pipeline completes successfully when all stages work as expected.
    """
    # Mock dependencies
    mock_extract_data.side_effect = [
        [{"customer_name": "John", "product": "Coffee", "price": "2.50"}],
        [{"customer_name": "Jane", "product": "Tea", "price": "1.75"}]
    ]
    mock_transform_data.return_value = {
        "final_transactions": [{"id": 1, "price": "2.50"}],
        "branch_data": {"branches_table": [], "transactions_with_branch_id": []},
        "product_data": {"products_table": [], "transactions_with_product_id": [], "transaction_product_table": []}
    }
    mock_load_data.return_value = None  # Simulate successful loading

    # Execute the pipeline
    result = run_etl_pipeline(valid_file_paths)

    # Assertions
    assert result is True
    mock_extract_data.assert_called()
    mock_transform_data.assert_called_once()
    mock_load_data.assert_called_once()


@patch("app.extract_data")
@patch("app.transform_data")
@patch("app.load_data")
def test_pipeline_no_extracted_data(mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths):
    """
    Test the ETL pipeline exits gracefully when no data is extracted.
    """
    # Mock extract_data to return empty lists
    mock_extract_data.side_effect = [[], []]

    # Execute the pipeline
    result = run_etl_pipeline(valid_file_paths)

    # Assertions
    assert result is False  # Pipeline should return False
    mock_extract_data.assert_called()
    mock_transform_data.assert_not_called()  # Transformation shouldn't happen
    mock_load_data.assert_not_called()  # Loading shouldn't happen


@patch("app.extract_data")
@patch("app.transform_data")
@patch("app.load_data")
def test_pipeline_transformation_failure(mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths):
    """
    Test the ETL pipeline handles transformation failure gracefully.
    """
    # Mock dependencies
    mock_extract_data.side_effect = [
        [{"customer_name": "John", "product": "Coffee", "price": "2.50"}]
    ]
    mock_transform_data.side_effect = Exception("Transformation failed!")

    # Execute the pipeline and expect an exception
    with pytest.raises(Exception, match="Transformation failed!"):
        run_etl_pipeline(valid_file_paths)

    # Assertions
    mock_extract_data.assert_called()
    mock_transform_data.assert_called_once()
    mock_load_data.assert_not_called()  # Loading shouldn't happen


@patch("app.extract_data")
@patch("app.transform_data")
@patch("app.load_data")
def test_pipeline_loading_failure(mock_load_data, mock_transform_data, mock_extract_data, valid_file_paths):
    """
    Test the ETL pipeline handles loading failure gracefully.
    """
    # Mock dependencies
    mock_extract_data.side_effect = [
        [{"customer_name": "John", "product": "Coffee", "price": "2.50"}]
    ]
    mock_transform_data.return_value = {
        "final_transactions": [{"id": 1, "price": "2.50"}],
        "branch_data": {"branches_table": [], "transactions_with_branch_id": []},
        "product_data": {"products_table": [], "transactions_with_product_id": [], "transaction_product_table": []}
    }
    mock_load_data.side_effect = Exception("Loading failed!")

    # Execute the pipeline and expect an exception
    with pytest.raises(Exception, match="Loading failed!"):
        run_etl_pipeline(valid_file_paths)

    # Assertions
    mock_extract_data.assert_called()
    mock_transform_data.assert_called_once()
    mock_load_data.assert_called_once()  # Loading should attempt and fail

