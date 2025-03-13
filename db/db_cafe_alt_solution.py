import os
import pymysql
import logging
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from utils.logger import get_logger

# Load environment variables from .env file
load_dotenv()

logger = get_logger("load", log_level=logging.DEBUG)

def load_data(
    transactions: List[Dict[str, Any]],
    branches: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    transaction_product: List[Dict[str, Any]]) -> None:
     """
    Loads transformed data into MySQL database tables for transactions, branches,
    products, and the transaction_product mapping table.

    This function connects to the database using credentials from environment variables,
    inserts data into four tables in a single connection using executemany for batch processing,
    commits the transaction, and closes the connection.

    Args:
        transactions (List[Dict[str, Any]]): A list of transactions with keys such as
            "date_time", "branch", "drink", "size", "price", "qty", "payment_type".
        branches (List[Dict[str, Any]]): A list of branch records with keys "id" and "name".
        products (List[Dict[str, Any]]): A list of product records with keys "id", "product_name",
            "size", "flavour", "price".
        transaction_product (List[Dict[str, Any]]): A list of mapping records with keys "id",
            "transaction_id", and "product_id".

    Returns:
        None

    Raises:
        Exception: If an error occurs during database operations.
    """
try:
    logger.info("Connecting to database...")
    # retrieve db credentials from environment variables
    with pymysql.connect(
            host=os.getenv("mysql_host", "localhost"),
            user=os.getenv("mysql_user", "root"),
            password=os.getenv("mysql_pass", ""),
            database=os.getenv("mysql_db", "cafe")
        ) as connection:

        logger.info("opening cursor...")
        cursor = connection.cursor()

        logger.info("Inserting new records into the database...")

         #Insert branches table
        branch_query = "INSERT INTO branches (id, name) VALUES (%s, %s)"
        branch_values = [
                (
                        b["id"], 
                        b["name"]
                )       
                for b in branches]
        cursor.executemany(branch_query, [tuple(b.values()) for b in branches])
        logger.info(f"Inserted {cursor.rowcount} records into branches.")

        # insert transaction table
        insert_query = """
        INSERT INTO transactions (id, date_time, branch_id, price, qty, payment_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        transaction_values = [
            (
                t["id"],
                t["date_time"],
                t["branch_id"],
                t["price"],
                t["qty"],
                t["payment_type"]
            )
            for t in transactions
        ]
        cursor.executemany(insert_query, [tuple(t.values()) for t in transactions])
        logger.info(f"Successfully loaded {cursor.rowcount} records into the database.")

         # Insert product table

        product_query = """
        INSERT INTO products (id, product_name, size, flavour, price)
        VALUES (%s, %s, %s, %s, %s)
        """
        product_values = [
            (
                    p["id"], 
                    p["product_name"], 
                    p["size"], 
                    p["flavour"], 
                    p["price"]
            ) 
            for p in products]
        cursor.executemany(product_query, [tuple(p.values()) for p in products])
        logger.info(f"Inserted {cursor.rowcount} records into products.")

        # Insert transaction_product table

        tp_query = """
        INSERT INTO transaction_product (id, transaction_id, product_id)
        VALUES (%s, %s, %s)
        """
        tp_values = [
            (
                    tp["id"], 
                    tp["transaction_id"], 
                    tp["product_id"]
            ) 
            for tp in transaction_product]
        cursor.executemany(tp_query, [tuple(tp.values()) for tp in transaction_product])
        logger.info(f"Inserted {cursor.rowcount} records into transaction_product.")


        connection.committ()
        cursor.close()
        connection.close()
        logger.info("Database connection closed.")

except Exception as e:
    logger.error(f"Error loading data: {e}")
