## How to get Setup:
**Step 1: Clone Repo**  
- git clone https://github.com/svc98/invoices_payments_reconciliation.git
- cd .../invoices_payments_reconciliation

**Step 2: Setup venv**  
- python3 -m venv venv
- source venv/bin/activate        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;                             # For Mac/Linux
- venv\Scripts\activate &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;         # For Windows

**Step 3: Install required packages and variables**  
- pip install -r requirements.txt
- cp variables.env .env


<br>

## How to Run:
**1. "python3 main.py"**
- Generates raw invoices and payments dates and drops off at /data/raw folder.
- Sets up the database and tables (Bronze, Silver, Gold layers)  
- Loads the raw data into Bronze tables.  
- Cleans, validates and loads the data into Silver tables.  
- Transforms and loads into analytics-ready Gold tables.  


**2. "python3 analysis.py"**  
- Execute sample business queries via SQL.  
- Create visualizations via Pandas/Matplotlib.  

<br>

## How to Query the DB:
**1. "sqlite3 ./db/invoices_payments.db"**    
**2. ".mode table" - this makes the table easy to view**  
**3. Query**

<br>

## Reason for picking the data:
After speaking with Nanette (Recruiter) about the challenges of reconciling payments between payers and providers, I became intrigued by the idea of invoicing and payment reconciliation. 
<p> When reviewing this assignment, I wanted to explore that topic more deeply, especially how real-world financial data often comes with inconsistencies, timing issues, and the need for robust ETL pipelines to manage it.
<p> I couldn't find a data source I liked, so I wrote a script to generate me invoices and payments to use with the help of the Faker library.