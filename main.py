from code.invoice_payment_gen import invoices_payments_data_gen

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



if __name__ == "__main__":
    # Create fake data to be dropped off at data/raw folder.
    generate_invoices_payments_data()

