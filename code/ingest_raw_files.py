import os
import pandas as pd
from datetime import datetime


def load_csv_to_df(path):
    # Read path and add load timestamp for historical purpose.
    df = pd.read_csv(path)
    df["load_timestamp"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return df

def insert_data(conn, cursor, df, table_name, id_column):
    # Get existing IDs already in the table. Bio O on Set lookup is faster, than list.
    existing_ids = set(row[0] for row in cursor.execute(f"SELECT {id_column} FROM {table_name};"))

    # Filter the raw DataFrame to only new IDs via filter masking.
    new_df = df[~df[id_column].isin(existing_ids)]

    # Either insert filtered df into db or is an empty df.
    if len(new_df) > 0:
        new_df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Inserted {len(new_df)} new records into {table_name}.")
    else:
        print(f"No new records to insert into {table_name}.")

def process_file(conn, cursor, file_path):
    # Identify which file (invoice or payment) to load
    if "invoices" in file_path:
        table_name = "bronze_invoices"
        id_column = "invoice_id"
    elif "payments" in file_path:
        table_name = "bronze_payments"
        id_column = "payment_id"
    else:
        print(f"Skipping unrecognized file: {file_path}")
        return

    # Load raw data into DataFrame
    df = load_csv_to_df(file_path)
    insert_data(conn, cursor, df, table_name, id_column)



##### Main Functions #####
def check_or_create_tables(conn, cursor, TABLE_SETUP_PATH, expected_tables):
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

def ingestion_start(conn, cursor, RAW_DATA_FOLDER, DB_PATH, TABLE_SETUP_PATH):
    # Check paths
    if not os.path.exists(RAW_DATA_FOLDER):
        raise FileNotFoundError(f"{RAW_DATA_FOLDER} not found.")
    elif not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"{DB_PATH} not found.")
    elif not os.path.exists(TABLE_SETUP_PATH):
        raise FileNotFoundError(f"{TABLE_SETUP_PATH} not found.")

    # Load raw CSV files into DataFrames
    for filename in os.listdir(RAW_DATA_FOLDER):
        full_path = os.path.join(RAW_DATA_FOLDER, filename)
        process_file(conn, cursor, full_path)