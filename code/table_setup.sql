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
    load_timestamp TEXT,
    is_cleaned INTEGER DEFAULT 0
);

CREATE TABLE bronze_payments (
    payment_id TEXT,
    invoice_id TEXT,
    due_date TEXT,
    payment_date TEXT,
    amount_due REAL,
    amount_paid REAL,
    load_timestamp TEXT,
    is_cleaned INTEGER DEFAULT 0
);

--------------------------------
-- SILVER LAYER: Cleaned & Validated Tables
DROP TABLE IF EXISTS silver_invoices;
DROP TABLE IF EXISTS silver_payments;

CREATE TABLE silver_invoices (
    invoice_id TEXT PRIMARY KEY,
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
    is_cleaned INTEGER DEFAULT 0
);

CREATE TABLE silver_payments (
    payment_id TEXT PRIMARY KEY,
    invoice_id TEXT,
    due_date TEXT,
    payment_date TEXT,
    amount_due REAL,
    amount_paid REAL,
    outstanding_balance REAL,
    is_cleaned INTEGER DEFAULT 0,
    FOREIGN KEY (invoice_id) REFERENCES silver_invoices(invoice_id)
);

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
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_gold_invoices_invoice_id ON invoices(invoice_id);
CREATE INDEX IF NOT EXISTS idx_gold_customers_customer_id ON customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_gold_departments_department_id ON departments(department_id);
