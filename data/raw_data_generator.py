import pandas as pd
import random
from datallm import DataLLM

# Initialize the DataLLM client
datallm = DataLLM()

# Function to generate full names
def generate_full_name():
    first_names = ["John", "Jane", "Alex", "Emily", "Michael", "Sarah"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Define the mock data generation
mock_data = datallm.mock(
    n=100,  # number of samples
    data_description="Generate mock transaction data for a coffee shop",
    columns={
        "Customer Name": {"prompt": "Full name of the customer", "dtype": "string"},
        "Drink": {"prompt": "Description of the drink ordered", "dtype": "string"},
        "Qty": {"prompt": "Quantity of drinks ordered", "dtype": "integer"},
        "Price": {"prompt": "Total price of the order", "dtype": "float"},
        "Branch": {"prompt": "Branch location of the coffee shop", "dtype": "string"},
        "Payment Type": {"prompt": "Type of payment used", "dtype": "category", "categories": ["CARD", "CASH"]},
        "Card Number": {"prompt": "Credit card number used for payment", "dtype": "string", "regex": "[0-9]{16}"},
        "Date/Time": {"prompt": "Date and time of the transaction", "dtype": "datetime"}
    },
    progress_bar=False
)

# Apply the full name generation function to ensure all names have both first and last names
mock_data['Customer Name'] = [generate_full_name() for _ in range(len(mock_data))]

# Save the generated mock data to a CSV file
mock_data_path = "/mnt/data/mock_transaction_data.csv"
mock_data.to_csv(mock_data_path, index=False)

# Print the path to the saved file
print(mock_data_path)





