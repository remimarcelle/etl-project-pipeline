"""
GUI Test Suite for ETLApp.

This test suite verifies the functionality of the ETLApp GUI logic
without rendering a visible window. It mocks subprocess calls to avoid
running the actual ETL pipeline, focusing on UI updates and user feedback.

Environment variables TCL_LIBRARY and TK_LIBRARY are loaded from .env
via python-dotenv to support tkinter on Windows machines.

# GUI tests disabled due to threading conflict in Tkinter (see README).

"""

import os
import subprocess

import platform
# if platform.system() == "Windows":
#     os.environ["TCL_LIBRARY"] = os.environ.get("TCL_LIBRARY", r"C:\Users\User\AppData\Local\Programs\Python\Python313\tcl\tcl8.6")
#     os.environ["TK_LIBRARY"] = os.environ.get("TK_LIBRARY", r"C:\Users\User\AppData\Local\Programs\Python\Python313\tcl\tk8.6")

from dotenv import load_dotenv
import pytest
import tkinter as tk
from unittest.mock import patch, MagicMock
from typing import Generator

load_dotenv()

from src.gui import ETLApp



@pytest.fixture
def app() -> Generator[ETLApp, None, None]:
    """
    Fixture that provides a hidden instance of ETLApp for testing.

    This avoids GUI rendering during test execution by using `root.withdraw()`.

    Yields:
        ETLApp: The GUI application instance.
    """
    root: tk.Tk = tk.Tk()
    # prevents actual window from rendering
    root.withdraw()  
    # gives the test a live GUI instance
    yield ETLApp(root, test_mode=True) 
    # cleanup after the test
    root.destroy() 

def create_mock_popen(returncode=0, stdout="Pipeline ran successfully", stderr=""):
    """
    Simulates a subprocess.Popen object with mock polling and communication.

    This is used to mimic asynchronous subprocess behaviour in the GUI's ETL logic.
    Args:
        returncode (int): The return code the process should exit with.
        stdout (str): Mocked standard output.
        stderr (str): Mocked standard error.

    Returns:
        MagicMock: A mocked subprocess.Popen-like object.
    """
    mock_process = MagicMock()
    mock_process.poll.side_effect = [None, None, returncode]
    mock_process.communicate.return_value = (stdout, stderr)
    mock_process.returncode = returncode
    return mock_process

@patch("src.gui.subprocess.Popen")
def test_run_etl_pipeline_success(mock_popen: MagicMock, app: ETLApp, capfd) -> None:
    """
    Test that the ETL pipeline runs successfully and updates the UI accordingly.

    Args:
        mock_subproc (MagicMock): Mocked subprocess.run returning successful result.
        app (ETLApp): The GUI application instance.
         
    Returns:
        None """
  
    mock_popen.return_value = create_mock_popen()
    
    app.start_etl()
    app.etl_thread.join() 

    out, _ = capfd.readouterr()
    assert "ETL pipeline finished successfully!" in out
    assert "Status: Completed" in out
    # assert "Pipeline ran successfully" in app.log_text.get("1.0", "end")
    # assert "Status: Completed" in app.status_label.cget("text")


@patch("src.gui.subprocess.Popen", side_effect=Exception("Boom!"))
def test_run_etl_pipeline_exception(mock_popen: MagicMock, app: ETLApp, capfd) -> None:
    """
    Tests an unexpected exception during ETL subprocess startup.

    Ensures that the GUI catches the exception, logs the error message,
    and updates the status label to 'Error'.

    Args:
        mock_popen (MagicMock): The mocked subprocess.Popen which raises an Exception.
        app (ETLApp): An instance of the ETL GUI application.

    Returns:
        None
    """
    app.start_etl()
    app.etl_thread.join() 

    out, _ = capfd.readouterr()
    assert "Unexpected error: Boom!" in out
    assert "Status: Error" in out
    # assert "Unexpected error: Boom!" in app.log_text.get("1.0", "end")
    # assert "Status: Error" in app.status_label.cget("text")


@patch("src.gui.subprocess.Popen")
def test_run_etl_pipeline_called_failure(mock_popen: MagicMock, app: ETLApp, capfd) -> None:
    """
    Tests a failed ETL pipeline run with a non-zero return code.

    Verifies that the GUI logs the stderr output from the subprocess and sets the status label to 'Error'.

    Args:
        mock_popen (MagicMock): The mocked subprocess.Popen object returning code 1.
        app (ETLApp): An instance of the ETL GUI application.

    Returns:
        None
    """
    mock_popen.side_effect = subprocess.CalledProcessError(
        returncode=1,
        cmd='python src/app.py',
        stderr="Something went wrong"
    )

    app.start_etl()
    app.etl_thread.join()  

    out, _ = capfd.readouterr()
    assert "ETL pipeline failed!" in out
    assert "Something went wrong" in out
    assert "Status: Error" in out
    # assert "ETL pipeline failed:" in app.log_text.get("1.0", "end")
    # assert "Something went wrong" in app.log_text.get("1.0", "end")
    # assert "Status: Error" in app.status_label.cget("text")