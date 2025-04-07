"""
GUI Test Suite for ETLApp.

This test suite verifies the functionality of the ETLApp GUI logic
without rendering a visible window. It mocks subprocess calls to avoid
running the actual ETL pipeline, focusing on UI updates and user feedback.

Environment variables TCL_LIBRARY and TK_LIBRARY are loaded from .env
via python-dotenv to support tkinter on Windows machines.
"""

import os
import subprocess

import platform
if platform.system() == "Windows":
    os.environ["TCL_LIBRARY"] = os.environ.get("TCL_LIBRARY", r"C:\Users\User\AppData\Local\Programs\Python\Python313\tcl\tcl8.6")
    os.environ["TK_LIBRARY"] = os.environ.get("TK_LIBRARY", r"C:\Users\User\AppData\Local\Programs\Python\Python313\tcl\tk8.6")

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
    yield ETLApp(root) 
    # cleanup after the test
    root.destroy() 

@patch("src.gui.subprocess.run")
def test_run_etl_pipeline_success(mock_subproc: MagicMock, app: ETLApp) -> None:
    """
    Test that the ETL pipeline runs successfully and updates the UI accordingly.

    Args:
        mock_subproc (MagicMock): Mocked subprocess.run returning successful result.
        app (ETLApp): The GUI application instance. """
  
    mock_subproc.return_value = MagicMock(
        stdout="Pipeline ran successfully",
        stderr="",
        returncode=0
    )

    app.run_etl_pipeline()

    assert "Pipeline ran successfully" in app.log_text.get("1.0", "end")
    assert "Status: Completed" in app.status_label.cget("text")


@patch("src.gui.subprocess.run", side_effect=Exception("Boom!"))
def test_run_etl_pipeline_generic_failure(mock_subproc: MagicMock, app: ETLApp) -> None:
    """
    Test that an unexpected error during subprocess.run is caught and shown in the GUI.

    Args:
        mock_subproc (MagicMock): Mocked subprocess.run raising a general Exception.
        app (ETLApp): The GUI application instance.
    """
    app.run_etl_pipeline()

    assert "Unexpected error: Boom!" in app.log_text.get("1.0", "end")
    assert "Status: Error" in app.status_label.cget("text")


@patch("src.gui.subprocess.run")
def test_run_etl_pipeline_called_process_error(mock_subproc: MagicMock, app: ETLApp) -> None:
    """
    Test that a CalledProcessError from subprocess.run is properly handled by the GUI.

    Args:
        mock_subproc (MagicMock): Mocked subprocess.run raising CalledProcessError.
        app (ETLApp): The GUI application instance.
    """
    mock_subproc.side_effect = subprocess.CalledProcessError(
        returncode=1,
        cmd='python src/app.py',
        stderr="Something went wrong"
    )

    app.run_etl_pipeline()

    assert "ETL pipeline failed:" in app.log_text.get("1.0", "end")
    assert "Something went wrong" in app.log_text.get("1.0", "end")
    assert "Status: Error" in app.status_label.cget("text")