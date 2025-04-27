import sqlite3
from code.invoice_payment_gen import invoices_payments_data_gen
from code.ingest_raw_files import check_or_create_tables


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

    except sqlite3.Error as e:
        print(f"SQLite error in check_or_create_bronze_tables: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if conn:
            conn.commit()
            conn.close()



if __name__ == "__main__":
    # Create fake data to be dropped off at data/raw folder.
    generate_invoices_payments_data()

    # Check if exists or Create database tables.
    check_or_create_db_tables()
