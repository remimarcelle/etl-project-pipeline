import os
import pymysql
import logging
from typing import List, Tuple, Any
from dotenv import load_dotenv
from utils.logger import get_logger

# Load environment variables from .env file.
load_dotenv()

logger = get_logger("db_inspect", log_level=logging.DEBUG)

def inspect_products() -> None:
    """
    Connects to the MySQL database, retrieves all product records from the
    'products' table, and prints selected product details (product name, size, flavour, and price).

    This utility is useful for verifying that data has been loaded correctly into the database.
    
    Returns:
        None

    Raises:
        Exception: If an error occurs during database connection or query execution.
    """
    try:
        logger.info("Connecting to database for product inspection...")
        conn = pymysql.connect(
            host=os.getenv("mysql_host", "localhost"),
            user=os.getenv("mysql_user", "root"),
            password=os.getenv("mysql_pass", ""),
            database=os.getenv("mysql_db", "cafe")
        )
        cursor = conn.cursor()

        # Construct a query to select product details.
        # Adjust this query if your schema uses different column names.
        query: str = """
            SELECT product_name, size, flavour, price
            FROM products
            ORDER BY id ASC
        """
        logger.info("Executing query: " + query)
        cursor.execute(query)

        # Fetch all rows from the query result.
        rows: List[Tuple[Any, ...]] = cursor.fetchall()
        logger.info("Retrieved products:")
        for row in rows:
            # Assuming the query returns columns in the order:
            # product_name, size, flavour, price.
            print(f"Product Name: {row[0]}, Size: {row[1]}, Flavour: {row[2]}, Price: {row[3]}")

        cursor.close()
        conn.close()
        logger.info("Database connection closed.")
    except Exception as e:
        logger.error(f"Failed to inspect products: {e}")
        raise

if __name__ == "__main__":
    inspect_products()
