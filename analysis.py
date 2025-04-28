import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables
load_dotenv('variables.env')

# Folder Paths
DB_PATH = os.getenv('DB_PATH')

conn = None                                              # In case something happens in the middle of the try block.
try:
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ----------------
    # Question 1: What are the top 5 customers by total amount paid? (SQL)
    # ----------------
    print("\nQuestion 1: Top 5 customers by total payments:")
    print("\nRationale: This tells us who our most valuable customers are — important for customer relationship management and potential upsell opportunities.\n")

    query1 = """
    SELECT 
        c.first_name || ' ' || c.last_name AS customer_name,
        SUM(p.amount_paid) AS total_paid
    FROM payments p
    JOIN invoices i ON p.invoice_id = i.invoice_id
    JOIN customers c ON i.customer_id = c.customer_id
    GROUP BY c.customer_id
    ORDER BY total_paid DESC
    LIMIT 5;
    """

    # Execute and display
    q1_result = pd.read_sql_query(query1, conn)
    print(q1_result)
    print("----------------------------------------------------------------------------------------------------------------------------------")


    # ----------------
    # Question 2: How many invoices are underpaid, exact paid, and overpaid? (SQL)
    # ----------------
    print("\nQuestion 2: Invoice Payment Status (Under Paid, Exact Paid, Over Paid)")
    print("\nRationale: Understanding payment behavior helps detect billing issues, customer payment trends, and possible accounting errors.\n")

    # Connect to database again
    conn = sqlite3.connect(DB_PATH)

    query2 = """
    WITH classified_invoices as (
        SELECT
            *,
            CASE 
                WHEN balance > 0 THEN 'under_paid'
                WHEN balance = 0 THEN 'exact_paid'
                WHEN balance < 0 THEN 'over_paid'
            END AS invoice_payment_status
        FROM invoices
        WHERE status IN ('Posted', 'Pending', 'Processing', 'Late')                           -- Exclude Cancelled invoices
    )
    
    SELECT invoice_payment_status, SUM(balance) as amount, COUNT(invoice_id) as count
    FROM classified_invoices
    GROUP BY invoice_payment_status
    ORDER BY 3 DESC;
    """

    q2_result = pd.read_sql_query(query2, conn)
    print(q2_result)
    print("----------------------------------------------------------------------------------------------------------------------------------")


    # ----------------
    # Question 3: What is the total revenue per department? (SQL)
    # ----------------
    print("\nQuestion 3: Total revenue by department")
    print("\nRationale: Understanding which departments drive the most revenue helps prioritize resource allocation and strategic decisions.\n")

    query3 = """
    SELECT 
        d.department_name,
        SUM(p.amount_paid) AS total_revenue
    FROM payments p
    JOIN invoices i ON p.invoice_id = i.invoice_id
    JOIN departments d ON i.department_id = d.department_id
    GROUP BY d.department_id
    ORDER BY total_revenue DESC;
    """

    q3_result = pd.read_sql_query(query3, conn)
    print(q3_result)
    print("----------------------------------------------------------------------------------------------------------------------------------")


    # ----------------
    # Question 4: Average payment amount? (SQL)
    # ----------------
    print("\nQuestion 4: Average Payment Amount")
    print("\nRationale: Knowing the average payment helps set realistic benchmarks and detect outliers.\n")

    query4 = """
    SELECT ROUND(AVG(amount_paid), 2) AS average_payment
    FROM payments;
    """
    q4_result = pd.read_sql_query(query4, conn)
    print(q4_result)
    print("----------------------------------------------------------------------------------------------------------------------------------")


    # ----------------
    # Question 5: What is the distribution of payments over time? (Python - pandas/matplotlib)
    # ----------------
    print("\nQuestion 5: Payment amount trends over time")
    print("\nRationale: Analyzing daily payment trends helps identify seasonality, payment patterns, or potential anomalies.\n")

    query5 = """
    SELECT 
        payment_date, 
        SUM(amount_paid) AS daily_total
    FROM payments
    GROUP BY payment_date
    ORDER BY payment_date;
    """
    payments_over_time = pd.read_sql_query(query5, conn)

    # Convert payment_date (TEXT) to pandas datetime
    payments_over_time['payment_date'] = pd.to_datetime(payments_over_time['payment_date'])

    # Plot
    plt.figure(figsize=(10, 6))
    plt.plot(payments_over_time['payment_date'], payments_over_time['daily_total'], marker='o')
    plt.title('Daily Payment Totals Over Time')
    plt.xlabel('Date')
    plt.ylabel('Total Payments')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    print("----------------------------------------------------------------------------------------------------------------------------------")

except Exception as e:
    print(f"An error occurred during analysis execution: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed.")