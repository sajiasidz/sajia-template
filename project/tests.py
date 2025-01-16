import os
import sqlite3
import pandas as pd
import subprocess

PIPELINE_SCRIPT = "./pipeline.py"  # Adjust the path to match the location of your pipeline script
DATABASE_PATH = "math_2006_2023.sqlite"

def test_pipeline_execution():
    """
    Test if the pipeline script executes successfully.
    """
    print("Testing pipeline execution...")
    try:
        result = subprocess.run(
            ["python", PIPELINE_SCRIPT],
            capture_output=True,
            text=True,
            check=True,
        )
        print("Pipeline executed successfully.")
        print("STDOUT:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Pipeline execution failed.")
        print("Return Code:", e.returncode)
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)
        raise

def test_database_creation():
    """
    Test if the SQLite database is created and contains the required tables.
    """
    print("Testing database creation...")
    assert os.path.exists(DATABASE_PATH), f"Database not found: {DATABASE_PATH}"

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Check for required tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    required_tables = ["Math_Results_2006_2012", "Math_Results_2013_2023"]
    for table in required_tables:
        assert table in tables, f"Table '{table}' not found in the database."

    # Check if tables have data
    for table in required_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        assert count > 0, f"Table '{table}' is empty."

    print("Database creation test passed.")
    conn.close()

def test_data_content():
    """
    Test if the database tables contain valid data.
    """
    print("Testing data content...")
    conn = sqlite3.connect(DATABASE_PATH)

    # Check for valid data in Math_Results_2006_2012
    df_2006_2012 = pd.read_sql_query("SELECT * FROM Math_Results_2006_2012;", conn)
    assert not df_2006_2012.empty, "Math_Results_2006_2012 table is empty."
    assert "Year" in df_2006_2012.columns, "'Year' column is missing in Math_Results_2006_2012."

    # Check for valid data in Math_Results_2013_2023
    df_2013_2023 = pd.read_sql_query("SELECT * FROM Math_Results_2013_2023;", conn)
    assert not df_2013_2023.empty, "Math_Results_2013_2023 table is empty."
    assert "Year" in df_2013_2023.columns, "'Year' column is missing in Math_Results_2013_2023."

    print("Data content test passed.")
    conn.close()

if __name__ == "__main__":
    print("Running pipeline tests...")
    test_pipeline_execution()
    test_database_creation()
    test_data_content()
    print("All tests passed.")
