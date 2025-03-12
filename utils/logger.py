import logging
import os

# Define the folder to store log files
LOG_FOLDER: str = "../logs"

# Ensure the logs folder exists
if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER, exist_ok=True)

def get_logger(module_name: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Creates and returns a configured logger instance for a specific module.

    Each module gets its own log file based on the module name. The log file will
    be created in the `logs/` folder, and log messages will be formatted consistently
    with timestamps, module names, log levels, and messages.

    Args:
        module_name (str): The name of the module requesting the logger 
            (e.g., 'extract', 'transform', 'load').

    Returns:
        logging.Logger: A configured logger instance for the specified module.
    """
    # Path to the log file for the specific module
    log_file: str = os.path.join(LOG_FOLDER, f"{module_name}.log")

    # Create and configure the logger
    logger: logging.Logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)  # Set the minimum logging level

    # Avoid adding multiple handlers for the same logger
    if not logger.handlers:
        # Create a file handler for writing logs to a module's log file
        file_handler: logging.FileHandler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)

        # Console handler for displaying logs in the terminal
        console_handler = logging.StreamHandler() = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # Define the log message format
        formatter: logging.Formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
