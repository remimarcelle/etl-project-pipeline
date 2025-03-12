import os
import pymysql
from pymysql.connections import Connection
import logging
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from utils.logger import get_logger

# Load environment variables from .env file
load_dotenv()

logger = get_logger("load", log_level=logging.DEBUG)

# Retrieve database credentials from environment variables.
mysql_host: str = os.environ.get("mysql_host", "localhost")
mysql_db: str = os.environ.get("mysql_db", "cafe")
mysql_user: str = os.environ.get("mysql_user", "root")
mysql_pass: str = os.environ.get("mysql_pass", "")

def connect_to_db() -> Optional[Connection]:
    """
    Establish a connection to the MySQL database using pymysql.
    
    Returns:
        Optional[Connection]: The database connection object if successful; otherwise, None.
    """
    try:
        connection: Connection = pymysql.connect(
            host=mysql_host,
            database=mysql_db,
            user=mysql_user,
            password=mysql_pass,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("Connected to the database successfully.")
        return connection
    except pymysql.MySQLError as e:
        logger.error(f"Error connecting to the database: {e}")
        return None

def load_branches(branches: List[Dict[str, Any]]) -> None:
    """
    Inserts branch records into the 'branches' table.
    
    Args:
        branches (List[Dict[str, Any]]): A list of branch records with keys "id" and "name".
    """
    connection = connect_to_db()
    if connection is None:
        logger.error("Branch loading aborted: No database connection.")
        return
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO branches (id, name) VALUES (%s, %s)"
            rows = [(branch["id"], branch["name"]) for branch in branches]
            cursor.executemany(sql, rows)
            connection.commit()
            logger.info(f"Inserted {cursor.rowcount} records into branches table.")
    except pymysql.MySQLError as e:
        logger.error(f"Error loading branches: {e}")
        raise
    finally:
        connection.close()
        logger.info("Database connection for branches closed.")

def load_products(products: List[Dict[str, Any]]) -> None:
    """
    Inserts product records into the 'products' table.
    
    Args:
        products (List[Dict[str, Any]]): A list of product records with keys
                                         "id", "product_name", "size", "flavour", "price".
    """
    connection = connect_to_db()
    if connection is None:
        logger.error("Product loading aborted: No database connection.")
        return
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO products (id, product_name, size, flavour, price)
            VALUES (%s, %s, %s, %s, %s)
            """
            rows = [(prod["id"], prod["product_name"], prod.get("size", ""), prod.get("flavour", ""), float(prod["price"])) for prod in products]
            cursor.executemany(sql, rows)
            connection.commit()
            logger.info(f"Inserted {cursor.rowcount} records into products table.")
    except pymysql.MySQLError as e:
        logger.error(f"Error loading products: {e}")
        raise
    finally:
        connection.close()
        logger.info("Database connection for products closed.")

def load_transactions(transactions: List[Dict[str, Any]]) -> None:
    """
    Inserts transaction records into the 'transactions' table.
    
    Args:
        transactions (List[Dict[str, Any]]): A list of transaction records with keys:
            "id", "branch_id", "date_time", "qty", "price", "payment_type".
    """
    connection = connect_to_db()
    if connection is None:
        logger.error("Transaction loading aborted: No database connection.")
        return
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO transactions (id, branch_id, date_time, qty, price, payment_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            rows = [
                (
                    txn["id"],
                    txn["branch_id"],
                    txn["date_time"],
                    int(txn["qty"]),
                    float(txn["price"]),
                    txn["payment_type"]
                )
                for txn in transactions
            ]
            cursor.executemany(sql, rows)
            connection.commit()
            logger.info(f"Inserted {cursor.rowcount} records into transactions table.")
    except pymysql.MySQLError as e:
        logger.error(f"Error loading transactions: {e}")
        raise
    finally:
        connection.close()
        logger.info("Database connection for transactions closed.")

def load_transaction_product(mapping: List[Dict[str, Any]]) -> None:
    """
    Inserts mapping records into the 'transaction_product' table.
    
    Args:
        mapping (List[Dict[str, Any]]): A list of mapping records with keys:
            "id", "transaction_id", "product_id".
    """
    connection = connect_to_db()
    if connection is None:
        logger.error("Mapping loading aborted: No database connection.")
        return
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO transaction_product (id, transaction_id, product_id)
            VALUES (%s, %s, %s)
            """
            rows = [(m["id"], m["transaction_id"], m["product_id"]) for m in mapping]
            cursor.executemany(sql, rows)
            connection.commit()
            logger.info(f"Inserted {cursor.rowcount} records into transaction_product table.")
    except pymysql.MySQLError as e:
        logger.error(f"Error loading transaction_product mappings: {e}")
        raise
    finally:
        connection.close()
        logger.info("Database connection for transaction_product closed.")

def main() -> None:
    """
    Main function for the load process.
    
    In your ETL pipeline, these data structures would be produced by your transform stage:
      - final_transactions: List[Dict[str, Any]]
      - branches: List[Dict[str, Any]]
      - products_table: List[Dict[str, Any]]
      - transaction_product_table: List[Dict[str, Any]]
      
    This function loads data into all the respective tables.
    """
    # In production, these variables should be passed from your transformation stage.
    final_transactions: List[Dict[str, Any]] = []          # Replace with actual transformed transactions.
    branches: List[Dict[str, Any]] = []                    # Replace with actual branches data.
    products_table: List[Dict[str, Any]] = []              # Replace with actual products data.
    transaction_product_table: List[Dict[str, Any]] = []   # Replace with actual mapping data.

    logger.info("Starting data load...")
    load_branches(branches)
    load_products(products_table)
    load_transactions(final_transactions)
    load_transaction_product(transaction_product_table)
    logger.info("Data load complete.")

if __name__ == "__main__":
    main()
