import os
import sqlite3
import pandas as pd
from pipeline import extract_data, transform1, data_load

def setup():
    """
    Setup the test environment.
    """
    data_dir = './data/raw_csv/'
    os.makedirs(data_dir, exist_ok=True)

def test_extract_data():
    """
    Test the data extraction phase.
    """
    data_2006_2012, data_2013_2023 = extract_data()
    assert isinstance(data_2006_2012, pd.DataFrame), "Extracted data_2006_2012 is not a DataFrame."
    assert isinstance(data_2013_2023, pd.DataFrame), "Extracted data_2013_2023 is not a DataFrame."
    assert len(data_2006_2012) > 0, "Extracted data_2006_2012 is empty."
    assert len(data_2013_2023) > 0, "Extracted data_2013_2023 is empty."

def test_transform1():
    """
    Test the transformation of the dataset from 2006 to 2012.
    """
    data_2006_2012, _ = extract_data()
    transformed_data = transform1(data_2006_2012)
    assert '% of Students Level 1' in transformed_data.columns, "Column '% of Students Level 1' not found."
    assert 's' not in transformed_data['% of Students Level 1'].unique(), "Invalid value 's' found in '% of Students Level 1'."

def test_data_load():
    """
    Test the data loading phase.
    """
    data_load()

    conn = sqlite3.connect('math_2006_2023.sqlite')
    cursor = conn.cursor()

    # Check for required tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    assert 'Math_Results_2006_2012' in tables, "Table 'Math_Results_2006_2012' not found."
    assert 'Math_Results_2013_2023' in tables, "Table 'Math_Results_2013_2023' not found."

    # Check for data in tables
    cursor.execute("SELECT COUNT(*) FROM Math_Results_2006_2012;")
    count_2006_2012 = cursor.fetchone()[0]
    assert count_2006_2012 > 0, "No data found in 'Math_Results_2006_2012'."

    cursor.execute("SELECT COUNT(*) FROM Math_Results_2013_2023;")
    count_2013_2023 = cursor.fetchone()[0]
    assert count_2013_2023 > 0, "No data found in 'Math_Results_2013_2023'."

    conn.close()

if __name__ == "__main__":
    print("Starting pipeline tests...")
    setup()
    try:
        test_extract_data()
        test_transform1()
        test_data_load()
        print("All tests passed successfully.")
    except AssertionError as e:
        print(f"Test failed: {e}")
