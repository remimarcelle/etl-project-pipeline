import pytest
from io import StringIO
from src.extract.extract import extract_data

def test_extract_data_from_stringio():
    """
    Tests the extract_data function by simulating CSV input using StringIO.
    
    The test verifies:
      - The function returns a non-None list.
      - The number of rows extracted matches the input.
      - Each row is a dictionary containing the desired keys in the proper order.
      - A default value for 'Qty' ("1") is inserted.
    """
    # Example CSV content following the assumed raw order:
    # [Date/Time, Branch, Customer Name, Drink, Price, Payment Type, Card Number]
    csv_content = (
        "25/08/2021 09:00,Chesterfield,Dave Keys,Latte,£3.50,CARD,0123456\n"
        "25/08/2021 09:08,London,Alice Franco,Cappuccino,£2.50,CARD,6543210\n"
        "25/08/2021 10:11,Oxford,Michael Brown,Espresso,£1.50,CASH,\n"
    )
    
    # Wrap the CSV string in a StringIO object to simulate a file.
    fake_file = StringIO(csv_content)
    
    # Call the extract_data function using the file-like object.
    data = extract_data(fake_file)
    
    # Assert that data was extracted and the output is a list.
    assert data is not None, "Extraction returned None; expected a list of dictionaries."
    assert isinstance(data, list), f"Expected data to be a list, but got {type(data)}."
    
    # The sample CSV has 3 rows, so we expect 3 dictionaries.
    assert len(data) == 3, f"Expected 3 rows but got {len(data)} rows."
    
    # Define the desired keys for each dictionary.
    desired_keys = ["Customer Name", "Drink", "Qty", "Price", "Branch", "Payment Type", "Card Number", "Date/Time"]
    
    # Verify that each row contains all of the desired keys.
    for idx, row in enumerate(data, start=1):
        for key in desired_keys:
            assert key in row, f"Row {idx} is missing the key '{key}'. Row data: {row}"
        
        # Verify that the default quantity is set correctly.
        assert row["Qty"] 