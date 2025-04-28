import os
import sqlite3
import pandas as pd
from datetime import datetime


def load_csv_to_df(path: str) -> pd.DataFrame:
    # Read path and add load timestamp for historical purpose.
    try:
        df = pd.read_csv(path)
        df["load_timestamp"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        return df
    except Exception as e:
        print(f"Error: Couldn't load CSV at {path}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def insert_data(conn: sqlite3.Connection, df: pd.DataFrame, table_name: str) -> None:
    try:
        # Directly insert the DataFrame into the bronze table without checking existing IDs
        if len(df) > 0:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Inserted {len(df)} records into {table_name}.")
        else:
            print(f"No new records to insert into {table_name}.")
    except Exception as e:
        print(f"Error: Couldnt inserting data into {table_name}: {e}")

def process_file(conn: sqlite3.Connection, cursor: sqlite3.Cursor, file_path: str) -> None:
    # Identify which file (invoice or payment) to load
    if "invoices" in file_path:
        table_name = "bronze_invoices"
    elif "payments" in file_path:
        table_name = "bronze_payments"
    else:
        print(f"Skipping unrecognized file: {file_path}")
        return

    # Load raw data into DataFrame
    df = load_csv_to_df(file_path)
    if not df.empty:
        insert_data(conn, df, table_name)
    else:
        print(f"Skipping empty file: {file_path}")



##### Main Functions #####
def check_or_create_tables(conn: sqlite3.Connection, cursor: sqlite3.Cursor, TABLE_SETUP_PATH: str, expected_tables: list[str]) -> None:
    # Iterate through SQLite tables and grab table name
    existing_tables = [row[0] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")]
    check = all(table.lower() in existing_tables for table in expected_tables)

    # If all values in check output to True then skip, else create tables.
    if not check:
        print("Schema missing, initializing...")
        with open(TABLE_SETUP_PATH, "r") as f:
            conn.executescript(f.read())
        print("Schema created.")
    else:
        print("Schema already exists. Skipping creation.")

def ingestion_start(conn: sqlite3.Connection, cursor: sqlite3.Cursor, RAW_DATA_FOLDER: str, DB_PATH: str, TABLE_SETUP_PATH: str) -> None:
    # Check paths
    if not os.path.exists(RAW_DATA_FOLDER):
        raise FileNotFoundError(f"Error: {RAW_DATA_FOLDER} not found.")
    elif not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Error: {DB_PATH} not found.")
    elif not os.path.exists(TABLE_SETUP_PATH):
        raise FileNotFoundError(f"Error: {TABLE_SETUP_PATH} not found.")

    # Load raw CSV files into DataFrames
    for filename in os.listdir(RAW_DATA_FOLDER):
        full_path = os.path.join(RAW_DATA_FOLDER, filename)
        process_file(conn, cursor, full_path)