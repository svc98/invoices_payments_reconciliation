import sqlite3
from datetime import datetime


def is_valid_date(date_string: str, date_format: str = "%Y-%m-%d") -> bool:
    try:
        # Try to parse the date string according to the specified format
        datetime.strptime(date_string, date_format)
        return True
    except ValueError:
        print(f"Error: Couldn't read date string")
        return False

def clean_enrich_bronze(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    # Step 1: Extract records where is_cleaned is 0
    cursor.execute("SELECT * FROM bronze_invoices WHERE is_cleaned = 0")
    bronze_invoices = cursor.fetchall()

    cursor.execute("SELECT * FROM bronze_payments WHERE is_cleaned = 0")
    bronze_payments = cursor.fetchall()

    # Step 2: Process bronze_invoices and insert into silver_invoices
    for invoice in bronze_invoices:
        invoice_id, customer_id, first_name, last_name, customer_email, customer_address, department, invoice_type, invoice_date, due_date, amount_due, currency, status, load_timestamp, is_cleaned = invoice

        # Apply transformation logic:
        # Handle null amounts or missing values
        amount_due = amount_due if amount_due is not None else 0.0

        # Handle invoice_date and due_date
        if invoice_date and is_valid_date(invoice_date):
            # Parse the date string into a datetime object
            date_obj = datetime.strptime(invoice_date, "%Y-%m-%d")

            # Reformat the date into the desired format
            invoice_date = date_obj.strftime("%m-%d-%Y")
        else:
            invoice_date = 'N/A'

        if due_date and is_valid_date(due_date):
            # Parse the date string into a datetime object
            date_obj = datetime.strptime(due_date, "%Y-%m-%d")

            # Reformat the date into the desired format
            due_date = date_obj.strftime("%m-%d-%Y")
        else:
            due_date = 'N/A'

        # Prepare SQL to insert into silver_invoices
        cursor.execute("""
        INSERT INTO silver_invoices (
            invoice_id, customer_id, first_name, last_name, customer_email, customer_address, department,
            invoice_type, invoice_date, due_date, amount_due, currency, status
        ) 
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM silver_invoices WHERE invoice_id = ?)
        """, (invoice_id, customer_id, first_name, last_name, customer_email, customer_address, department,
              invoice_type, invoice_date, due_date, amount_due, currency, status, invoice_id))

    # Step 3: Process bronze_payments and insert into silver_payments
    for payment in bronze_payments:
        payment_id, invoice_id, due_date, payment_date, amount_due, amount_paid, load_timestamp, is_cleaned = payment

        # Apply transformation logic:
        # Handle null amounts or missing values
        amount_paid = amount_paid if amount_paid is not None else 0.0

        # Handle due_date and payment_date
        if due_date and is_valid_date(due_date):
            # Parse the date string into a datetime object
            date_obj = datetime.strptime(due_date, "%Y-%m-%d")

            # Reformat the date into the desired format
            due_date = date_obj.strftime("%m-%d-%Y")
        else:
            due_date = 'N/A'

        if payment_date and is_valid_date(payment_date):
            # Parse the date string into a datetime object
            date_obj = datetime.strptime(payment_date, "%Y-%m-%d")

            # Reformat the date into the desired format
            payment_date = date_obj.strftime("%m-%d-%Y")
        else:
            payment_date = 'N/A'
        # Prepare SQL to insert into silver_payments
        cursor.execute("""
        INSERT INTO silver_payments (payment_id, invoice_id, due_date, payment_date, amount_due, amount_paid) 
        SELECT ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM silver_payments WHERE payment_id = ?)
        """, (payment_id, invoice_id, due_date, payment_date, amount_due, amount_paid, payment_id))

    # Step 4: Update is_cleaned to 1 in both bronze_invoices and bronze_payments tables
    cursor.executemany("UPDATE bronze_invoices SET is_cleaned = 1 WHERE invoice_id = ?", [(invoice[0],) for invoice in bronze_invoices])
    cursor.executemany("UPDATE bronze_payments SET is_cleaned = 1 WHERE payment_id = ?",
                       [(payment[0],) for payment in bronze_payments])

    # Step 5: Commit the changes to the database
    conn.commit()
    print(f"Inserted {len(bronze_invoices)} invoices and {len(bronze_payments)} payments.")