from src.extract.extract import extract_data
from src.transform.transform import transform_data
from src.load.load import load_data
from src.logger import get_logger

logger = get_logger("app")

def main():
    """
    Executes the full ETL pipeline by combining extract, transform, and load steps.
    """
    try:
        logger.info("Starting the ETL pipeline...")

        # Extract
        data = extract_data()

        # Transform
        transformed_data = transform_data(data)

        # Load
        db_config = {"host": "localhost", 
                     "user": "root", 
                     "password": "root", 
                     "database": "cafe"}
        load_data(transformed_data, db_config)

        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
