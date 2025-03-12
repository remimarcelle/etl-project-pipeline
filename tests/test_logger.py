import logging
import os
from utils.logger import get_logger

LOG_FOLDER: str = "logs"
log_file_path: str = os.path.join(LOG_FOLDER, "logger.log")

# Ensure that the logs folder exists
if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER, exist_ok=True)

def test_generate_logs_for_modules() -> None:
    """Tests that logger.py writes all test log messages into logger.log.

    This test obtains a logger via get_logger using the fixed module name "logger",
    writes log messages at levels DEBUG, INFO, WARNING, ERROR, and CRITICAL, flushes and
    closes the FileHandler(s), and then verifies that:
    
      - The consolidated log file logger.log is created.
      - The file contains all expected log messages.
    
    Returns:
        None
    """
    print(f"Expected log file path: {log_file_path}")

    # create a logger with module name "logger" so that FIleHandler writes it
    logger: logging.Logger = get_logger("logger", log_level=logging.DEBUG)

    # Generate log messages at all levels.
    logger.debug("Debug log - testing logger configuration")
    logger.info("Info log - testing logger configuration")
    logger.warning("Warning log - testing logger configuration")
    logger.error("Error log - testing logger configuration")
    logger.critical("Critical log - testing logger configuration")

    # Flush and close file handlers so that logs are written immediately.
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
                handler.flush()
                handler.close()

    assert os.path.exists(log_file_path), f"Expected log file {log_file_path} does not exist!"

    # read and verify the contents of the log file.
    with open(log_file_path, "r", encoding="utf-8") as log_file:
        logs = log_file.read()
        assert "Debug log - testing logger configuration" in logs, "DEBUG log message missing!"
        assert "Info log - testing logger configuration" in logs, "INFO log message missing!"
        assert "Warning log - testing logger configuration" in logs, "WARNING log message missing!"
        assert "Error log - testing logger configuration" in logs, "ERROR log message missing!"
        assert "Critical log - testing logger configuration" in logs, "CRITICAL log message missing!"



