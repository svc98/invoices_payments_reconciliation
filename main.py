import os
import shutil
import sqlite3
from code.invoice_payment_gen import invoices_payments_data_gen
from code.ingest_raw_files import check_or_create_tables, ingestion_start


# Folder Paths
RAW_DATA_FOLDER = './data/raw/'
PROCESSED_FOLDER = './data/processed/'
DB_PATH = "./db/invoices_payments.db"
TABLE_SETUP_PATH = "./code/table_setup.sql"
expected_tables = ["bronze_invoices", "bronze_payments", "silver_invoices", "silver_payments", "customers", "departments", "invoices"]


def generate_invoices_payments_data():
    try:
        # Generate raw data
        invoices_payments_data_gen()
    except Exception as e:
        print(f"Error while generating invoices and payments data: {e}")
        print("------")

def check_or_create_db_tables():
    conn = None                                              # In case something happens in the middle of the try block.
    try:
        # Create DB connection and cursor
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Enable foreign key support
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Check or create new tables. Expected 1st run create. After only check.
        check_or_create_tables(conn, cursor, TABLE_SETUP_PATH, expected_tables)
        print("Tables checked/created successfully.")
        print("------")

    except sqlite3.Error as e:
        print(f"SQLite error in check_or_create_bronze_tables: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Ensure connection is closed even if there's an error
        if conn:
            conn.close()

def ingest_new_files_to_bronze():
    conn = None                                              # In case something happens in the middle of the try block.
    try:
        # First check if there are any expected raw files
        files_to_process = [filename for filename in os.listdir(RAW_DATA_FOLDER)
                            if ("invoices" in filename or "payments" in filename) and filename.endswith('.csv')]

        # If there are raw files, then begin processing.
        if files_to_process:
            # Create DB connection and cursor
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            ingestion_start(conn, cursor, RAW_DATA_FOLDER, DB_PATH, TABLE_SETUP_PATH)
            for filename in files_to_process:
                full_path = os.path.join(RAW_DATA_FOLDER, filename)
                shutil.move(full_path, os.path.join(PROCESSED_FOLDER, filename))

                print(f"{filename} moved to processed folder.")
                print("------")

        else:
            print(f"No files found at {RAW_DATA_FOLDER}")

    except FileNotFoundError as e:
        print(f"File not found error: {e}")
    except sqlite3.Error as e:
        print(f"SQLite error in ingest_new_files: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Ensure connection is closed even if there's an error
        if conn:
            conn.close()



if __name__ == "__main__":
    # Create fake data to be dropped off at data/raw folder.
    generate_invoices_payments_data()

    # Check if exists or Create database tables.
    check_or_create_db_tables()

    # Ingest from data/raw folder into SQlite db, then move to data/processed folder.
    ingest_new_files_to_bronze()
