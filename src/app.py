import tkinter as tk
from tkinter import filedialog, messagebox
from typing import List
from src.extract.extract import extract_data
from src.transform.transform import transform_data
from db.db_cafe_alt_solution import load_data
from utils.logger import get_logger

logger = get_logger("app", log_level=logging.DEBUG)

def run_etl_pipeline(file_paths: List[str]) -> None:
    """
    Executes the ETL pipeline for the provided CSV file paths.

    Args:
        file_paths (List[str]): A list of CSV file paths selected by the user.
        
    Raises:
        Exception: If any step of the ETL pipeline fails.
    """
    try:
        extracted_data: List = []
        # Process all selected files.
        for file_path in file_paths:
            logger.info(f"Extracting data from {file_path}...")
            data = extract_data(file_path)  # extract_data expects a file path or file-like object
            if data:
                extracted_data.extend(data)
        
        if not extracted_data:
            raise Exception("No data extracted from the selected files.")

        # Transform the aggregated extracted data.
        transformed_data = transform_data(extracted_data)
        if not transformed_data:
            raise Exception("No data produced after transformation.")

        # Load data into the database using the consolidated load_data function.
        load_data(transformed_data)
        logger.info("ETL pipeline completed successfully.")
        messagebox.showinfo("Success", "ETL pipeline completed successfully!")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        messagebox.showerror("Error", f"ETL pipeline failed: {e}")
        raise

def select_files_and_run() -> None:
    """
    Opens a file dialog for the user to select CSV files, then runs the ETL pipeline on the selected files.
    
    Returns:
        None
    """
    file_paths: List[str] = list(
        filedialog.askopenfilenames(
            title="Upload CSV Reports",
            filetypes=[("CSV Files", "*.csv")]
        )
    )
    if file_paths:
        run_etl_pipeline(file_paths)
    else:
        messagebox.showwarning("No Selection", "No files were selected.")

def main() -> None:
    """
    Creates and runs the Tkinter GUI for executing the ETL pipeline.
    
    Returns:
        None
    """
    root: tk.Tk = tk.Tk()
    root.title("ETL Pipeline Runner")
    root.geometry("300x150")
    
    run_button: tk.Button = tk.Button(root, text="Start Programme", command=select_files_and_run)
    run_button.pack(expand=True)
    
    root.mainloop()