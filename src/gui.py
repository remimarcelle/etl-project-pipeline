import os
import platform
import sys
from dotenv import load_dotenv
# Load environment variables for Windows-specific Tk/Tcl paths
load_dotenv("db/.env")

if platform.system() == "Windows":
    tcl_path = os.getenv("TCL_LIBRARY")
    tk_path = os.getenv("TK_LIBRARY")

    if not tcl_path or not tk_path:
        raise EnvironmentError("TCL_LIBRARY or TK_LIBRARY not set in .env")
    
    os.environ["TCL_LIBRARY"] = tcl_path
    os.environ["TK_LIBRARY"] = tk_path

print("Running from:", sys.executable)
from typing import Optional, List
import threading
import subprocess
import time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

class ETLApp:
    def __init__(self, root: tk.Tk, test_mode: bool = False) -> None:
        """
        Initialises the ETLApp GUI.

        Args:
               root (tk.Tk): The root window of the application.
         """
        self.root = root
        self.test_mode = test_mode
        self.root.title("ETL Pipeline Runner")
        self.root.geometry("800x500")
        self.create_menu()
        self.create_widgets()
        self.etl_thread = None
        self.stop_event = threading.Event()
        self.process = None

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
        file_menu.add_command(label="Run Test Dataset", command=lambda: self.start_etl())
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)
        
        # Help menu with an About dialog
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about)
        help_menu.add_command(label="How to Use", command=self.show_help)
        menubar.add_cascade(label="Help", menu=help_menu)

    def show_about(self) -> None:
        """
        Displays information about the program.
        """
        messagebox.showinfo("About", "ETL Pipeline Runner v1.0\nDeveloped for RMB-DE-ETLproject Generation UK & Ireland 2025")

    def show_help(self):
        messagebox.showinfo(
            "How to Use",
            "1. Click 'Run ETL Pipeline' to choose CSV files to process.\n"
            "2. The system will extract, clean, and upload your data to the database.\n"
            "3. Test mode (no file selected) will skip database upload.\n"
            "4. Visit http://localhost:8080 to view data in Adminer."
        )


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
        self.run_button = tk.Button(control_frame, text="Run Test Dataset", command=lambda: self.start_etl(), width=20)
        self.run_button.pack(pady=10)
        
        self.stop_button = tk.Button(control_frame, text="Stop Processing", command=self.stop_etl, state="disabled", width=20)
        self.stop_button.pack(pady=10)

        self.upload_button = tk.Button(control_frame, text="Run My Files", command=self.select_files_and_run, width=20)
        self.upload_button.pack(pady=10)

        self.status_label = tk.Label(control_frame, text="Status: Idle", width=20, relief="groove")
        self.status_label.pack(pady=10)

        self.selected_files_label = tk.Label(
            control_frame,
            text="Selected file(s): None",
            wraplength=180,
            justify="left"
        )
        self.selected_files_label.pack(pady=(5, 10))

        
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
                title="Select csv files to process",
                filetypes=[("CSV Files", "*.csv")]
            )
        )
        if file_paths:
            display_paths = "\n".join([os.path.basename(path) for path in file_paths])
            self.selected_files_label.config(text=f"Selected file(s):\n{display_paths}")
            self.start_etl(list(file_paths))
        else:
            self.selected_files_label.config(text="Selected file(s): None")
            messagebox.showwarning("No Selection", "No files were selected.")

    def start_etl(self, file_paths: Optional[List[str]] = None) -> None:
        """
        Starts the ETL pipeline in a background thread.

        This method ensures that each ETL run begins with a clean state by clearing any
        previous stop flags. It then disables the Run button, enables the Stop button,
        updates the status label, shows the progress bar, and starts a new thread to
        run the ETL subprocess asynchronously.

        Args:
            file_paths (Optional[List[str]]): List of CSV file paths to process. If None,
            app.py will use its own default dataset logic."""
        self.stop_event.clear()
        # Update buttons and status
        self.run_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.status_label.config(text="Status: Running programme...")
        self.progress.start(10)
        self.log("Starting programme...")

        # Start the ETL pipeline in a separate thread to keep the GUI responsive
        self.etl_thread = threading.Thread(target=self.run_etl_pipeline, args=(file_paths,))
        self.etl_thread.start()

    # def stop_etl(self):
    #     """
    #     Displays a placeholder for stopping the ETL (not implemented). COMMENTED IT OUT BECAUSE
    #     ITS A PLACEHOLDER AND WILL BE USEFUL FOR ANOTHER TIME.
    #     """
    #     # For now, we'll simply simulate a stop action as proper termination might require process management
    #     messagebox.showinfo("Stop programme", "Stop functionality is not implemented yet.")
    #     self.run_button.config(state="normal")
    #     self.stop_button.config(state="disabled")
    #     self.progress.stop()
    #     self.status_label.config(text="Status: Idle")

    def stop_etl(self) -> None:
        """
        Attempts to stop the currently running ETL subprocess.

        This method checks if the ETL thread (self.etl_thread) is still alive,
        and if so, sets a thread-safe stop_event flag to signal the ETL loop to exit.
        If a subprocess (self.process) is still active, it attempts to terminate it using .terminate().

        After attempting to stop the process, this method also resets GUI buttons and status indicators.

        Note:
        This method only works correctly if run_etl_pipeline has saved a reference
        to self.process using subprocess.Popen().

        Args:
            None

        Raises:
            No exception is raised if the process is already terminated, but a log entry is made.
        """
        if self.etl_thread and self.etl_thread.is_alive():
            self.log("Stop requested. Attempting to terminate ETL process...")
            self.status_label.config(text="Status: Stopping...")
            self.stop_event.set()

            if self.process and self.process.poll() is None:
                try:
                    self.process.terminate()
                except Exception as e:
                    self.log(f"Failed to terminate process: {e}")


    # def run_etl_pipeline(self, file_paths: Optional[List[str]] = None) -> None:
    #     """
    #     Runs the ETL pipeline via subprocess.

    #     Args:
    #         file_paths (Optional[List[str]]): Optional list of CSV file paths.
    #     """
    #     try:
    #         # Run your ETL pipeline by calling app.py (which orchestrates the ETL process)
    #         # It's assumed that app.py lives in src/ and is executable via: python src/app.py
    #         # The subprocess.run call captures the pipeline's output.
    #         command = ["python", "src/app.py"]
    #         if file_paths:
    #             command.extend(file_paths)

    #         process = subprocess.run(
    #             command,
    #             capture_output=True, text=True, check=True
    #         )
    #         if process.stdout:
    #             self.log(process.stdout)
    #         if process.stderr:
    #             self.log("Errors:\n" + process.stderr)
    #         self.log("ETL pipeline finished successfully!")
    #         self.status_label.config(text="Status: Completed")
    #     except subprocess.CalledProcessError as e:
    #         self.log("ETL pipeline failed:")
    #         self.log(e.stderr)
    #         self.status_label.config(text="Status: Error")
    #     except Exception as ex:
    #         self.log("Unexpected error: " + str(ex))
    #         self.status_label.config(text="Status: Error")
    #     finally:
    #         self.run_button.config(state="normal")
    #         self.stop_button.config(state="disabled")
    #         self.progress.stop()

    def run_etl_pipeline(self, file_paths: Optional[List[str]] = None) -> None:
        """
        Runs the ETL pipeline via subprocess with using the same
        Python interpreter as the GUI.

        This method constructs the ETL command using sys.executable to ensure it runs in
        the same virtual environment or interpreter as the GUI. It launches app.py with any
        provided CSV file paths via subprocess.Popen(), and stores the process reference in
        self.process so it can be terminated later by the stop_etl() method.

        Args:
            file_paths (Optional[List[str]]): List of CSV file paths selected by the user.
            If None, the app will run on the default dataset defined inside app.py.
        """
        try:
            command = [sys.executable, "src/app.py"]
            if file_paths:
                command.extend(file_paths)

            self.log(f"Launching ETL with: {command}")
            self.log(f"Using Python interpreter: {sys.executable}")


            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Wait loop with cancellation support
            while True:
                if self.stop_event.is_set():
                    self.log("ETL pipeline interrupted by user.")
                    self.process.terminate()
                    break

                retcode = self.process.poll()
                if retcode is not None:  # Process finished
                    stdout, stderr = self.process.communicate()
                    if stdout:
                        self.log(stdout)
                    if stderr:
                        self.log("Errors:\n" + stderr)

                    if retcode == 0:
                        self.log("ETL pipeline finished successfully!")
                        self.status_label.config(text="Status: Completed")
                    else:
                        self.log("ETL pipeline failed!")
                        self.status_label.config(text="Status: Error")
                    break

                time.sleep(0.2)

        except Exception as ex:
            self.log("Unexpected error: " + str(ex))
            self.status_label.config(text="Status: Error")
        finally:
            self.run_button.config(state="normal")
            self.stop_button.config(state="disabled")
            self.progress.stop()
            self.status_label.config(text="Status: Idle")

    def log(self, message: str) -> None:
        # Append log messages with a timestamp to the log_text widget
        timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
        full_msg = "end", f"{timestamp} {message}\n"
        if self.test_mode:
            print(full_msg)  # Visible in pytest terminal output
        else:
            self.log_text.insert("end", full_msg)
            self.log_text.see("end")  

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
