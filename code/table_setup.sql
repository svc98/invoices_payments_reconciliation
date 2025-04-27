-- Table Setup of Bronze, Silver and Gold

--------------------------------
-- BRONZE LAYER: Raw Staging Tables
DROP TABLE IF EXISTS bronze_invoices;
DROP TABLE IF EXISTS bronze_payments;

CREATE TABLE bronze_invoices (
    invoice_id TEXT,
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    customer_email TEXT,
    customer_address TEXT,
    department TEXT,
    invoice_type TEXT,
    invoice_date TEXT,
    due_date TEXT,
    amount_due REAL,
    currency TEXT,
    status TEXT,
    load_timestamp TEXT
);

CREATE TABLE bronze_payments (
    payment_id TEXT,
    invoice_id TEXT,
    due_date TEXT,
    payment_date TEXT,
    amount_due REAL,
    amount_paid REAL,
    amount_remaining REAL,
    load_timestamp TEXT
);

--------------------------------
-- SILVER LAYER: Cleaned & Validated Tables
DROP TABLE IF EXISTS silver_invoices;
DROP TABLE IF EXISTS silver_payments;

CREATE TABLE silver_invoices (
    invoice_id TEXT PRIMARY KEY,
    customer_id TEXT,
    department TEXT,
    invoice_type TEXT,
    invoice_date TEXT,
    due_date TEXT,
    amount_due REAL,
    currency TEXT,
    status TEXT,
    load_timestamp TEXT
);

CREATE TABLE silver_payments (
    payment_id TEXT PRIMARY KEY,
    invoice_id TEXT,
    payment_date TEXT,
    amount_paid REAL,
    amount_remaining REAL,
    load_timestamp TEXT,
    FOREIGN KEY (invoice_id) REFERENCES silver_invoices(invoice_id)
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_silver_invoices_customer_id ON silver_invoices(customer_id);
CREATE INDEX IF NOT EXISTS idx_silver_payments_invoice_id ON silver_payments(invoice_id);


--------------------------------
-- GOLD LAYER: Analytical Tables
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS gold_invoice_summary;

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    customer_email TEXT,
    customer_address TEXT
);

CREATE TABLE departments (
    department_id TEXT PRIMARY KEY,
    department_name TEXT
);

CREATE TABLE invoices (
    invoice_id TEXT PRIMARY KEY,
    customer_id TEXT,
    department_id TEXT,
    invoice_type TEXT,
    invoice_date TEXT,
    due_date TEXT,
    amount_due REAL,
    amount_paid REAL,
    amount_remaining REAL,
    status TEXT,
    currency TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (department_name) REFERENCES departments(department_name)
);