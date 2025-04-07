import os
import platform
import sys
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from typing import Optional, List
import threading
import subprocess
import time
from dotenv import load_dotenv

# Load environment variables for Windows-specific Tk/Tcl paths
load_dotenv()
if platform.system() == "Windows":
    os.environ.setdefault("TCL_LIBRARY", os.getenv("TCL_LIBRARY", ""))
    os.environ.setdefault("TK_LIBRARY", os.getenv("TK_LIBRARY", ""))

print("Running from:", sys.executable)

class ETLApp:
    def __init__(self, root: tk.Tk) -> None:
        """
        Initialises the ETLApp GUI.

        Args:
            root (tk.Tk): The root window of the application.
        """
        self.root = root
        self.root.title("ETL Pipeline Runner")
        self.root.geometry("800x500")
        self.create_menu()
        self.create_widgets()
        self.etl_thread = None

    def create_menu(self) -> None:
        """
        Creates the top menu bar for the GUI.

        I used a lambda to ensure that my start_etl() method, which accepts optional arguments, 
        is safely invoked from the menu without triggering a TypeError.
        """
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu for running ETL and exiting
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Run Programme", command=lambda: self.start_etl())
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)
        
        # Help menu with an About dialog
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about)
        menubar.add_cascade(label="Help", menu=help_menu)

    def show_about(self) -> None:
        """
        Displays information about the program.
        """
        messagebox.showinfo("About", "ETL Pipeline Runner v1.0\nDeveloped for RMB-DE-ETLproject Generation UK & Ireland 2025")

    def create_widgets(self) -> None:
        """
        Creates and lays out all GUI widgets.
        """
        # Main container with two frames: one for controls and one for displaying logs
        main_frame = tk.Frame(self.root)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Create a left frame for control buttons and status
        control_frame = tk.Frame(main_frame, bd=2, relief="sunken")
        control_frame.pack(side="left", fill="y", padx=(0,10))
        
        # Create a right frame for log display
        log_frame = tk.Frame(main_frame, bd=2, relief="sunken")
        log_frame.pack(side="right", fill="both", expand=True)

        # Control frame widgets
        self.run_button = tk.Button(control_frame, text="Run ETL Pipeline", command=lambda: self.start_etl(), width=20)
        self.run_button.pack(pady=10)
        
        self.stop_button = tk.Button(control_frame, text="Stop ETL Pipeline", command=self.stop_etl, state="disabled", width=20)
        self.stop_button.pack(pady=10)

        self.upload_button = tk.Button(control_frame, text="Upload CSVs & Run", command=self.select_files_and_run, width=20)
        self.upload_button.pack(pady=10)

        self.status_label = tk.Label(control_frame, text="Status: Idle", width=20, relief="groove")
        self.status_label.pack(pady=10)
        
        self.progress = ttk.Progressbar(control_frame, orient="horizontal", mode="indeterminate", length=150)
        self.progress.pack(pady=10)
        
        # Log frame widgets
        self.log_text = tk.Text(log_frame, wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)
        
        scrollbar = tk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def select_files_and_run(self) -> None:
        """
        Opens a file dialog for the user to select CSV files, then runs the ETL pipeline on the selected files.
    
        Returns:
            None
        """
        file_paths: List[str] = list(
            filedialog.askopenfilenames(
                title="Upload CSV files",
                filetypes=[("CSV Files", "*.csv")]
            )
        )
        if file_paths:
            self.start_etl(list(file_paths))
        else:
            messagebox.showwarning("No Selection", "No files were selected.")

    def start_etl(self, file_paths: Optional[List[str]] = None) -> None:
        """
        Starts the ETL pipeline in a separate thread.

        Args:
            file_paths (Optional[List[str]]): List of CSV file paths to process.
        """
        # Update buttons and status
        self.run_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.status_label.config(text="Status: Running programme...")
        self.progress.start(10)
        self.log("Starting programme...")

        # Start the ETL pipeline in a separate thread to keep the GUI responsive
        self.etl_thread = threading.Thread(target=self.run_etl_pipeline, args=(file_paths,))
        self.etl_thread.start()

    def stop_etl(self):
        """
        Displays a placeholder for stopping the ETL (not implemented).
        """
        # For now, we'll simply simulate a stop action as proper termination might require process management
        messagebox.showinfo("Stop programme", "Stop functionality is not implemented yet.")
        self.run_button.config(state="normal")
        self.stop_button.config(state="disabled")
        self.progress.stop()
        self.status_label.config(text="Status: Idle")

    def run_etl_pipeline(self, file_paths: Optional[List[str]] = None) -> None:
        """
        Runs the ETL pipeline via subprocess.

        Args:
            file_paths (Optional[List[str]]): Optional list of CSV file paths.
        """
        try:
            # Run your ETL pipeline by calling app.py (which orchestrates the ETL process)
            # It's assumed that app.py lives in src/ and is executable via: python src/app.py
            # The subprocess.run call captures the pipeline's output.
            command = ["python", "src/app.py"]
            if file_paths:
                command.extend(file_paths)

            process = subprocess.run(
                command,
                capture_output=True, text=True, check=True
            )
            if process.stdout:
                self.log(process.stdout)
            if process.stderr:
                self.log("Errors:\n" + process.stderr)
            self.log("ETL pipeline finished successfully!")
            self.status_label.config(text="Status: Completed")
        except subprocess.CalledProcessError as e:
            self.log("ETL pipeline failed:")
            self.log(e.stderr)
            self.status_label.config(text="Status: Error")
        except Exception as ex:
            self.log("Unexpected error: " + str(ex))
            self.status_label.config(text="Status: Error")
        finally:
            self.run_button.config(state="normal")
            self.stop_button.config(state="disabled")
            self.progress.stop()

    def log(self, message: str) -> None:
        # Append log messages with a timestamp to the log_text widget
        timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
        self.log_text.insert("end", f"{timestamp} {message}\n")
        self.log_text.see("end")  # Automatically scroll to the bottom

if __name__ == "__main__":
    root = tk.Tk()
    app = ETLApp(root)
    root.mainloop()



# def main() -> None:
#     """
#     Creates and runs the Tkinter GUI for executing the ETL pipeline.
    
#     Returns:
#         None
#     """
#     root: tk.Tk = tk.Tk()
#     root.title("ETL Pipeline Runner")
#     root.geometry("300x150")
    
#     run_button: tk.Button = tk.Button(root, text="Start Programme", command=select_files_and_run)
#     run_button.pack(expand=True)
    
#     root.mainloop()
