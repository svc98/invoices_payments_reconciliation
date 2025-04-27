import csv
import random
import uuid
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker(locale='en_US')                                            # Library to generate fake data
chaos_threshold = 0.025
invoice_count = 100
payment_count = 25
timestamp = datetime.now().strftime("%Y_%m_%d")                         # for file outputting --> invoice_2025_04_25.csv
invoices = []


## Invoices Gen
# Step 1: Generate invoices
for i in range(1, invoice_count + 1):
    first_name = fake.first_name()
    last_name = fake.last_name()
    invoice_date = fake.date_between(start_date="-35d", end_date="-7d")
    due_date = invoice_date + timedelta(days=30)
    current_date = datetime.today().date()
    amount_due = round(random.uniform(100, 5000), 2)
    status = random.choices(["Posted", "Pending", "Processing", "Canceled"], weights=[0.7, 0.1, 0.15, 0.05])[0]

    # Random add Missing Values
    if status == 'Posted' and random.random() <= chaos_threshold:
        amount_due = None

    # Randomly add future dates
    if status == 'Pending' and random.random() <= chaos_threshold:
        invoice_date = current_date + timedelta(days=random.randint(7, 30))

    # Create invoice & randomly add duplicates
    if invoices and random.random() <= chaos_threshold:
        invoices.append(invoices[-1])
    else:
        ## Final Checks
        # Set Late status on current date past the due date.
        if current_date > due_date:
            status = "Late"

        invoices.append({
            "invoice_id": f"INV-{i}-{uuid.uuid1()}",
            'customer_id': str(uuid.uuid4()),
            "first_name": first_name,
            "last_name": last_name,
            "customer_email": first_name + last_name[0] + random.choice(["@yahoo.com", "@gmail.com", "@outlook.com"]),
            "customer_address": fake.address(),
            "invoice_type": random.choices(['Subscription', 'Product', 'Consulting', 'Training', 'Maintenance', 'Onboarding'], weights=[0.25, 0.25, 0.25, 0.1, 0.05, 0.05])[0],
            "invoice_date": invoice_date.strftime("%Y-%m-%d"),
            "due_date": due_date.strftime("%Y-%m-%d"),
            "amount_due": amount_due,
            "currency": "USD",
            "status": status
        })

print(str(invoice_count) + " invoices generated.")

# Step 2: Save invoices to CSV
# Generate the timestamp suffix

# Create the filename with the timestamp
invoice_filename = f"data/raw/invoices_{timestamp}.csv"
with open(invoice_filename, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=invoices[0].keys())
    writer.writeheader()
    writer.writerows(invoices)


## Payments Gen
# Step 3: Generate payments randomly linked to the above invoices
invoices_df = pd.DataFrame(invoices)
posted_df = invoices_df[(invoices_df['status'] == 'Posted') & (invoices_df['amount_due'].notnull())].reset_index(
    drop=True)
paid_df = posted_df.sample(n=payment_count, random_state=1).reset_index(drop=True)
paid_records = paid_df.to_dict(orient='records')

payments = []
for i, invoice in enumerate(paid_records):
    due_date = datetime.strptime(invoice["due_date"], "%Y-%m-%d")

    # Payment date: +/- 15 days from due date
    payment_date = due_date + timedelta(days=random.randint(-15, 15))

    # Simulate full (60%), partial (20%), or overpayment (20%) --- Generates a list of 100 values to select from.
    variance_options = (
            [round(random.uniform(0.5, 0.9), 2) for _ in range(20)] +   # underpaid
            [1.0 for _ in range(60)] +                                        # exact payment (most common)
            [round(random.uniform(1.1, 1.4), 2) for _ in range(20)]     # overpaid
    )
    payment_variance = random.choice(variance_options)
    amount_paid = round(invoice["amount_due"] * payment_variance, 2)

    payments.append({
        "payment_id": f"PAY-{i}-{uuid.uuid1()}",
        "invoice_id": invoice['invoice_id'],
        "due_date": invoice['due_date'],
        "payment_date": payment_date.strftime("%Y-%m-%d"),
        "amount_due": invoice['amount_due'],
        "amount_paid": amount_paid,
        "amount_remaining": round(invoice['amount_due'] - amount_paid, 2)
    })

# Step 4: Save payments to CSV
payments_filename = f"data/raw/payments_{timestamp}.csv"
with open(payments_filename, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=payments[0].keys())
    writer.writeheader()
    writer.writerows(payments)

print(str(payment_count) + " payments generated.")

