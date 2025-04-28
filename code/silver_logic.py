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


##### Main Function #####
def move_bronze_to_silver(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    try:
        # Track counts
        invoices_moved_to_silver = 0
        payments_moved_to_silver = 0

        # Step 1: Extract records where is_cleaned is 0
        cursor.execute("SELECT * FROM bronze_invoices WHERE is_cleaned = 0")
        bronze_invoices = cursor.fetchall()

        cursor.execute("SELECT * FROM bronze_payments WHERE is_cleaned = 0")
        bronze_payments = cursor.fetchall()

        # Step 2: Process bronze_invoices and insert into silver_invoices
        for invoice in bronze_invoices:
            invoice_id, customer_id, first_name, last_name, customer_email, customer_address, invoice_type, invoice_date, due_date, amount_due, currency, status, load_timestamp, is_cleaned = invoice

            # Apply transformation logic:
            # Handle null amounts or missing values
            if amount_due is None or amount_due == 0:
                continue

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
                invoice_id, customer_id, first_name, last_name, customer_email, customer_address,
                invoice_type, invoice_date, due_date, amount_due, currency, status
            ) 
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM silver_invoices WHERE invoice_id = ?)
            """, (invoice_id, customer_id, first_name, last_name, customer_email, customer_address,
                  invoice_type, invoice_date, due_date, amount_due, currency, status, invoice_id))

            # Update is_cleaned to 1 for the current invoice. Update row counter.
            cursor.execute("UPDATE bronze_invoices SET is_cleaned = 1 WHERE invoice_id = ?", (invoice_id,))
            invoices_moved_to_silver += 1

        # Step 3: Process bronze_payments and insert into silver_payments
        for payment in bronze_payments:
            payment_id, invoice_id, due_date, payment_date, amount_due, amount_paid, load_timestamp, is_cleaned = payment

            # Apply transformation logic:
            # Handle null amounts or missing values
            if amount_paid is None or amount_paid == 0:
                continue

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

            # Update is_cleaned to 1 for the current payment. Update row counter.
            cursor.execute("UPDATE bronze_payments SET is_cleaned = 1 WHERE payment_id = ?", (payment_id,))
            payments_moved_to_silver += 1

        # Step 4: Commit the changes to the database
        conn.commit()
        print(f"Inserted {invoices_moved_to_silver} invoices and {payments_moved_to_silver} payments into Silver Layer")

    except Exception as e:
        print(f"Error: Database error in move_bronze_to_silver: {e}")
        conn.rollback()