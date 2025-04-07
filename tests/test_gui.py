import pytest
from unittest.mock import MagicMock, patch
from gui import ETLApp

@pytest.fixture
def mock_tk_root():
    with patch("tkinter.Tk") as MockTk:
        yield MockTk.return_value

@pytest.fixture
def app(mock_tk_root):
    return ETLApp(mock_tk_root)

def test_gui_initialization(app):
    assert app.root.title.called_with("ETL Pipeline Runner")

def test_start_etl(app):
    with patch.object(app, 'run_etl_pipeline') as mock_run_pipeline:
        app.start_etl()
        mock_run_pipeline.assert_called_once()
        assert app.run_button['state'] == "disabled"
        assert app.stop_button['state'] == "normal"
        assert app.status_label['text'] == "Status: Running programme..."

def test_stop_etl(app):
    with patch("tkinter.messagebox.showinfo") as mock_info:
        app.stop_etl()
        mock_info.assert_called_with("Stop programme", "Stop functionality is not implemented yet.")
        assert app.run_button['state'] == "normal"
        assert app.stop_button['state'] == "disabled"
