import json
import sqlite3
from datetime import datetime
from typing import Any


def safe_float(value: Any) -> float:
    # Just an easy way to assess if a value is a float or not.
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def insert_into_gold_customers(cursor: sqlite3.Cursor, customer_id: str, first_name: str, last_name: str, customer_email: str, customer_address: str) -> None:
    try:
        # If customer doesn't exist insert, else skip.
        cursor.execute("SELECT 1 FROM customers WHERE customer_id = ?", (customer_id,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("""
                INSERT INTO customers (customer_id, first_name, last_name, customer_email, customer_address)
                VALUES (?, ?, ?, ?, ?)
            """, (customer_id, first_name, last_name, customer_email, customer_address))
    except sqlite3.Error as e:
        print(f"Error: Database error in insert_into_gold_customers: {e}")
    except Exception as e:
        print(f"Error: Unexpected error in insert_into_gold_customers: {e}")

def determine_gold_department_classification(cursor: sqlite3.Cursor, invoice_type: str, DEPARTMENT_MAPPINGS_PATH: str) -> int:
    try:
        # Load department mappings from the config file
        with open(DEPARTMENT_MAPPINGS_PATH, 'r') as file:
            department_mappings = json.load(file)

        # Search for the department based on the invoice type
        department_name = None
        for dept, types in department_mappings.items():
            if invoice_type in types:
                department_name = dept
                break

        # Query the gold_departments table to find the department_id
        cursor.execute("SELECT department_id FROM departments WHERE department_name = ?", (department_name,))
        department_id = cursor.fetchone()

        # If no department found, log an error and return None
        if department_id:
            return department_id[0]
        else:
            # If department is not found, insert into the table and return the new department_id
            print(f"Department '{department_name}' not found in departments. Inserting new department.")

            # Insert new department into gold_departments table
            cursor.execute("INSERT INTO departments (department_name) VALUES (?)", (invoice_type,))

            # Fetch the newly inserted department_id (assuming department_id is auto-incremented)
            cursor.execute("SELECT department_id FROM departments WHERE department_name = ?", (invoice_type,))
            department_id = cursor.fetchone()

            # Return the newly inserted department_id
            return department_id[0]

    except (sqlite3.Error, json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error: loading department mappings or processing invoice: {e}")
    except Exception as e:
        print(f"Error: Unexpected error in determine_gold_department_classification: {e}")


def insert_into_gold_invoices(cursor: sqlite3.Cursor, invoice_id: str, customer_id: str, department_id: int, invoice_type: str, invoice_date: str, due_date: str,
                              amount_due: Any, amount_paid: Any, balance: Any, currency: str, status: str) -> None:
    try:
        # If invoice doesn't exist insert, else skip.
        cursor.execute("SELECT 1 FROM invoices WHERE invoice_id = ?", (invoice_id,))
        exists = cursor.fetchone()

        if not exists:
            # Current Time
            now = str(datetime.now().strftime("%m-%d-%Y-%H-%M-%S"))

            # Prepare SQL for insertion
            cursor.execute("""
                INSERT INTO invoices (
                    invoice_id, customer_id, department_id, invoice_type, invoice_date, due_date, 
                    amount_due, amount_paid, balance, currency, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (invoice_id, customer_id, department_id, invoice_type, invoice_date, due_date,
                  safe_float(amount_due), safe_float(amount_paid), safe_float(balance), currency, status, now, now))
    except sqlite3.Error as e:
        print(f"Database error in insert_into_gold_invoices: {e}")
    except Exception as e:
        print(f"Error: Unexpected error in insert_into_gold_invoices: {e}")

def insert_into_gold_payments(cursor: sqlite3.Cursor, payment_id: str, invoice_id: str, payment_date: str, amount_due: Any, amount_paid: Any) -> None:
    try:
        # If payment doesn't exist insert, else skip.
        cursor.execute("SELECT 1 FROM payments WHERE payment_id = ?", (payment_id,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("""
                INSERT INTO payments (payment_id, invoice_id, payment_date, amount_paid)
                VALUES (?, ?, ?, ?)
            """, (payment_id, invoice_id, payment_date, safe_float(amount_paid)))

            # Calculate the balance
            balance = safe_float(amount_due) - safe_float(amount_paid)

            # Update the invoices table with the new amount_paid and balance
            cursor.execute("""
                UPDATE invoices
                SET amount_paid = ?, balance = ?
                WHERE invoice_id = ?
            """, (safe_float(amount_paid), safe_float(balance), invoice_id))

    except sqlite3.Error as e:
        print(f"Database error in insert_into_gold_payments: {e}")
    except Exception as e:
        print(f"Error: Unexpected error in insert_into_gold_payments: {e}")


##### Main Function #####
def move_silver_to_gold(conn: sqlite3.Connection, cursor: sqlite3.Cursor, DEPARTMENT_MAPPINGS_PATH: str) -> None:
    try:
        # Track counts
        invoices_moved_to_gold = 0
        payments_moved_to_gold = 0

        # Select data from silver_invoices
        cursor.execute("SELECT * FROM silver_invoices WHERE is_cleaned = 0")
        silver_invoices = cursor.fetchall()


        for invoice in silver_invoices:
            # Grab invoice values
            invoice_id, customer_id, first_name, last_name, customer_email, customer_address, invoice_type, invoice_date, due_date, amount_due, currency, status, is_cleaned = invoice

            # Insert into customer
            insert_into_gold_customers(cursor, customer_id, first_name, last_name, customer_email, customer_address)

            # Figure out department
            department_id = determine_gold_department_classification(cursor, invoice_type, DEPARTMENT_MAPPINGS_PATH)

            # Insert into invoice: Setting amount_paid = 0 and balance = amount_due, will come back later and update accordingly.
            insert_into_gold_invoices(cursor, invoice_id, customer_id, department_id, invoice_type, invoice_date, due_date,
                                      amount_due, 0.0, amount_due, currency, status)

            # Update is_cleaned to 1 for the current invoice. Update row counter.
            cursor.execute("UPDATE silver_invoices SET is_cleaned = 1 WHERE invoice_id = ?", (invoice_id,))
            invoices_moved_to_gold += 1

        # Select data from silver_payments
        cursor.execute("SELECT * FROM silver_payments WHERE is_cleaned = 0")
        silver_payments = cursor.fetchall()

        for payment in silver_payments:
            # Grab payment values
            payment_id, invoice_id, due_date, payment_date, amount_due, amount_paid, is_cleaned = payment

            # Insert or update the payment in gold layer
            insert_into_gold_payments(cursor, payment_id, invoice_id, payment_date, amount_due, amount_paid)

            # Update is_cleaned to 1 for the current invoice. Update row counter.
            cursor.execute("UPDATE silver_payments SET is_cleaned = 1 WHERE payment_id = ?", (payment_id,))
            payments_moved_to_gold += 1

        # Commit transaction to the database
        conn.commit()
        print(f"Inserted {invoices_moved_to_gold} invoices and {payments_moved_to_gold} payments into Gold Layer")

    except Exception as e:
        print(f"Error: Database error in move_silver_to_gold: {e}")
        conn.rollback()