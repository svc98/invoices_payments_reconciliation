import os
from dotenv import load_dotenv
import shutil
import sqlite3
from code.invoice_payment_gen import invoices_payments_data_gen
from code.bronze_logic import ingestion_start, check_or_create_tables
from code.silver_logic import clean_enrich_bronze

# Load environment variables
load_dotenv('variables.env')

# Folder Paths
RAW_DATA_FOLDER = os.getenv('RAW_DATA_FOLDER')
PROCESSED_FOLDER = os.getenv('PROCESSED_FOLDER')
DB_PATH = os.getenv('DB_PATH')
TABLE_SETUP_PATH = os.getenv('TABLE_SETUP_PATH')
expected_tables = ["bronze_invoices", "bronze_payments", "silver_invoices", "silver_payments", "customers", "departments", "invoices"]

# Data Gen
chaos_threshold = os.getenv('CHAOS_THRESHOLD')
invoice_count = os.getenv('INVOICE_COUNT')
payment_count = os.getenv('PAYMENT_COUNT')

def generate_invoices_payments_data() -> None:
    """
    Generate raw invoices and payments data using the fake data generator via Faker library.

    This function invokes the invoices_payments_data_gen function to create raw invoices and payments data.
    Then it outputs that data to the RAW_DATA_FOLDER as a csv file.
    If an error occurs during the generation, it prints an error message.
    """

    try:
        # Generate raw data
        invoices_payments_data_gen(chaos_threshold, invoice_count, payment_count)
        print("------")

    except Exception as e:
        print(f"Error: While generating invoices and payments data: {e}")

def check_or_create_db_tables() -> None:
    """
    Check if the required SQLite database tables exist, and create them if necessary.

    This function connects to the SQLite database, checks for the existence of expected tables, and creates them if they are not found.
    It also enables foreign key support in the SQLite database.
    If any errors occur during the process, they are caught and printed.
    """

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
        print(f"Error: SQLite error in check_or_create_bronze_tables: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure connection is closed even if there's an error
        if conn:
            conn.close()

def ingest_new_files_to_bronze() -> None:
    """
    Ingest new CSV files from the raw data folder into the bronze layer.

    This function searches for CSV files in the RAW_DATA_FOLDER that contain 'invoices' or 'payments' in their name,
         then processes them by calling the ingestion_start function.
    The ingestion_start function reads the files and mass ingests into bronze layer.
    After processing, the files are moved to the PROCESSED_FOLDER. If no files are found, a message is printed.
    """

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
        print(f"Error: File not found: {e}")
    except sqlite3.Error as e:
        print(f"Error: SQLite error in ingest_new_files: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure connection is closed even if there's an error
        if conn:
            conn.close()

def move_new_bronze_records_to_silver() -> None:
    """
    Move newly processed bronze records to the silver table.

    This function cleans and enriches the records in the bronze layer and moves them to the silver layer for further processing.
    "is_cleaned" = 0 is used to determine if a records hasn't been processed/load into silver layer. Default is 0.
    "is_cleaned" = 1 is used to determine if it has.
    Any errors encountered during the process are printed.
    """

    conn = None                                              # In case something happens in the middle of the try block.
    try:
        # Create DB connection and cursor
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Clean and Enrich bronze records in order to move to Silver
        clean_enrich_bronze(conn, cursor)
        print("------")

    except sqlite3.Error as e:
        print(f"Error: SQLite error in move_new_bronze_records_to_silver: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure connection is closed even if there's an error
        if conn:
            conn.close()



if __name__ == "__main__":
    """
    Main entry point for the data processing pipeline.

    This script is responsible for:
    1. Generating raw invoices and payments data.
    2. Checking and creating necessary database tables.
    3. Ingesting new CSV files from the raw data folder into the bronze layer.
    4. Moving cleaned and enriched bronze records to the silver layer.
    5. Moving records in silver layer to gold layer.

    Each of these steps is executed in sequence with error handling in place.
    """

    # Create fake data to be dropped off at data/raw folder.
    generate_invoices_payments_data()

    # Check if exists or Create database tables.
    check_or_create_db_tables()

    # Ingest files from data/raw folder into bronze, then move files to data/processed folder.
    ingest_new_files_to_bronze()

    # Bronze to Silver.
    move_new_bronze_records_to_silver()