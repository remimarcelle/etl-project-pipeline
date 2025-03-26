import os
import pymysql
import logging
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from utils.logger import get_logger

# Load environment variables from .env file
load_dotenv()

logger = get_logger(__name__, log_level=logging.DEBUG)

def create_db_connection() -> pymysql.connections.Connection:
    """Creates and returns a new database connection."""
    return pymysql.connect(
        host=os.getenv("mysql_host", "localhost"),
        user=os.getenv("mysql_user", "root"),
        password=os.getenv("mysql_pass", ""),
        database=os.getenv("mysql_db", "cafe")
    )

def load_data(transformed_data: Dict[str, Any], connection: pymysql.connections.Connection) -> None:
    """
    Loads transformed data into MySQL database tables for transactions, branches,
    products, and the transaction_product mapping table.

    The transformed_data dictionary is expected to contain the following keys:
        "final_transactions": A list of transactions with keys such as "id", "branch_id", "date_time", 
                                                                "price", "qty", and "payment_type".
        "branch_data": A dictionary with keys:
            - "branches_table": A list of branch records (each with "id" and "name").
            - "transactions_with_branch_id": A list of transactions updated with "branch_id".
        "product_data": A dictionary with keys:
            - "products_table": A list of product records with keys "id", "product_name",
                                "size", "flavour", and "price".
            - "transactions_with_product_id": A list of transactions updated with "product_id".
            - "transaction_product_table": A list of mappings with keys "id", "transaction_id",
                                             and "product_id".

    Args:
        transformed_data (Dict[str, Any]): The transformed data produced by the ETL pipeline.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the database operations.
    """
    # Extract the parts from the transformed data dictionary.
    final_transactions = transformed_data["final_transactions"]
    branch_data = transformed_data["branch_data"]
    product_data = transformed_data["product_data"]

    # Define variables for clarity.
    branches = branch_data.get("branches_table", [])
    transactions = final_transactions  # Final transactions are already updated.
    products = product_data.get("products_table", [])
    transaction_product = product_data.get("transaction_product_table", [])

    try:
        logger.info("Starting database insertion...")

        with connection.cursor() as cursor:
            # Insert branches explicitly
            cursor.executemany(
                "INSERT INTO branches (id, name) VALUES (%s, %s)",
                [(b["id"], b["name"]) for b in branches]
            )
            logger.info(f"Inserted {cursor.rowcount} records into branches.")

            # Insert transactions explicitly
            cursor.executemany(
                "INSERT INTO transactions (branch_id, date_time, price, qty, payment_type) VALUES (%s, %s, %s, %s, %s)",
                [(t["branch_id"], t["date_time"], t["price"], t["qty"], t["payment_type"]) for t in transactions]
            )
            logger.info(f"Inserted {cursor.rowcount} records into transactions.")

            # Insert products explicitly
            cursor.executemany(
                "INSERT INTO products (id, product_name, size, flavour, price) VALUES (%s, %s, %s, %s, %s)",
                [(p["id"], p["product_name"], p["size"], p["flavour"], p["price"]) for p in products]
            )
            logger.info(f"Inserted {cursor.rowcount} records into products.")

            # Insert transaction-product mapping explicitly
            cursor.executemany(
                "INSERT INTO transaction_product (id, transaction_id, product_id) VALUES (%s, %s, %s)",
                [(tp["id"], tp["transaction_id"], tp["product_id"]) for tp in transaction_product]
            )
            logger.info(f"Inserted {cursor.rowcount} records into transaction_product.")

        connection.commit()
        logger.info("Data loaded and committed successfully.")

    except Exception as e:
        logger.error(f"Error during database insertion: {e}")
        connection.rollback()
        raise
