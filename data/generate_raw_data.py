import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict

# --- Step 1: Define Sample Data Lists ---
# These lists provide sample values for each field.
first_names = ["Dave", "Alice", "Michael", "Sarah", "Mark", "Emma", "Tom", "James", "Olivia", "Noah", "Lydia", "Molly", "Fred"]
last_names = ["Keys", "Franco", "Brown", "Smith", "Johnson", "Wilson", "White", "Taylor", "Green", "Clark", "Bakerson", "Grayson"]
# Drink choices: we have simple options and "detailed" options that contain commas
drinks_simple = ["Latte", "Cappuccino", "Espresso", "Mocha", "Flat White", "Americano", "Macchiato", "Tea", "Cortado", "Hot Chocolate"]
drinks_detailed = [
    "Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45",  # requires quotes
    "Regular Latte - 2.15, Large Latte - 2.45",  # requires quotes
    "Regular Flavoured iced latte - Caramel - 2.75"
    "Regular Latte - Vanilla"  # no comma, so quotes not necessary
]
branches = ["Chesterfield", "London", "Oxford", "Brighton", "Birmingham", "Manchester", "Leeds", "Cambridge", "Bristol"]
payment_types = ["CARD", "CASH"]
# Prices as strings (some with £ symbol, some without)
prices = ["£3.50", "£2.50", "£1.50", "3.50", "4.50", "3.75", "3.25", "2.50", "3.80", "3.60"]

# --- Step 2: Date and Time Generation ---
# We'll generate a random transaction date/time as a string.
# For this example, we'll specify a start date and generate times within a 3-hour window.
start_datetime = datetime(2021, 8, 25, 9, 0)  # 25/08/2021 09:00

def random_date() -> str:
    """
    Generates a random date/time string within a 3-hour window from the start date.
    The format will be "dd/mm/yyyy HH:MM".
    """
    delta_minutes = random.randint(0, 180)
    transaction_time = start_datetime + timedelta(minutes=delta_minutes)
    return transaction_time.strftime("%d/%m/%Y %H:%M")

# --- Step 3: Card Number Generation ---
def random_card_number() -> str:
    """
    Generates a random 16-digit card number as a string.
    """
    return ''.join(str(random.randint(0, 9)) for _ in range(16))

# --- Step 4: Row Generation Functions ---
def generate_normal_row() -> Dict[str, str]:
    """
    Generates a normal, correctly formatted row.
    
    The order of fields is:
    Customer Name, Drink, Qty, Price, Branch, Payment Type, Card Number, Date/Time.
    For the Drink field, sometimes a 'detailed' description is used which will typically need quotes.
    """
    first = random.choice(first_names)
    last = random.choice(last_names)
    customer_name = f"{first} {last}"
    
    # Randomly decide to use a detailed drink or a simple drink.
    if random.random() < 0.5:
        # Use detailed drink which might include commas.
        drink_val = random.choice(drinks_detailed)
    else:
        drink_val = random.choice(drinks_simple)
    # We let csv.DictWriter handle quotes automatically (it will quote if the field contains commas)
    
    qty = str(random.choice([1, 2, 3]))
    price = random.choice(prices)
    branch = random.choice(branches)
    payment = random.choice(payment_types)
    # For CARD payments, generate a card number; for CASH, leave it blank.
    card_number = random_card_number() if payment == "CARD" else ""
    date_time = random_date()
    
    return {
        "Customer Name": customer_name,
        "Drink": drink_val,
        "Qty": qty,
        "Price": price,
        "Branch": branch,
        "Payment Type": payment,
        "Card Number": card_number,
        "Date/Time": date_time
    }

def generate_malformed_row() -> Dict[str, str]:
    """
    Generates a row with deliberate malformations, such as missing fields.
    Randomly removes one of the fields to simulate a malformed row.
    """
    row = generate_normal_row()
    
    # Choose one field to blank out
    field_to_remove = random.choice(["Drink", "Qty", "Price", "Branch"])
    row[field_to_remove] = ""
    
    return row

# --- Step 5: Generate Data and Write CSV ---
def generate_csv(filename: str, total_rows: int = 350, malformed_rate: float = 0.1) -> None:
    """
    Generates a CSV file with randomly generated data.
    
    Args:
        filename (str): The output filename (should be placed in the data/ directory).
        total_rows (int): Total number of rows to generate.
        malformed_rate (float): Fraction of rows that should be intentionally malformed.
    """
    header = ["Customer Name", "Drink", "Qty", "Price", "Branch", "Payment Type", "Card Number", "Date/Time"]
    rows: List[Dict[str, str]] = []
    
    # Optionally, you can include some fixed rows (if desired).
    # Here we'll simply generate random rows.
    for _ in range(total_rows):
        if random.random() < malformed_rate:
            row = generate_malformed_row()
        else:
            row = generate_normal_row()
        rows.append(row)
    
    # Write data to CSV using DictWriter, which automatically handles quoting minimally.
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Generated {total_rows} rows of test data in {filename}")

if __name__ == "__main__":
    generate_csv("data/raw-data.csv", total_rows=350, malformed_rate=0.1)

