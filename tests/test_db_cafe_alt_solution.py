import pytest
import uuid
import logging
from unittest.mock import patch, MagicMock
from typing import Dict, Any
from db.db_cafe_alt_solution import load_data

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def sample_transformed_data() -> Dict[str, Any]:
    transaction_id = str(uuid.uuid4())
    branch_id = str(uuid.uuid4())
    product_id = str(uuid.uuid4())
    transaction_product_id = str(uuid.uuid4())

    return {
        "final_transactions": [
            {"branch_id": branch_id, "date_time": "2024-11-27 10:00:00", "price": 3.5, "qty": 2, "payment_type": "Card"}
        ],
        "branch_data": {
            "branches_table": [{"id": branch_id, "name": "Main Branch"}],
            "transactions_with_branch_id": []
        },
        "product_data": {
            "products_table": [
                {"id": product_id, "product_name": "Coffee", "size": "Medium", "flavour": "Regular", "price": 3.5}
            ],
            "transactions_with_product_id": [],
            "transaction_product_table": [
                {"id": transaction_product_id, "transaction_id": transaction_id, "product_id": product_id}
            ]
        }
    }


@patch("db.db_cafe_alt_solution.pymysql.connect")
def test_load_data(mock_connect: MagicMock, sample_transformed_data: Dict[str, Any]) -> None:
    mock_connection = mock_connect.return_value
    mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

    load_data(sample_transformed_data, mock_connection)

    mock_connection.cursor.assert_called_once()
    assert mock_cursor.executemany.call_count == 4, "Expected inserts into four tables"
    mock_connection.commit.assert_called_once()

    logger.info("load_data function tested successfully.")