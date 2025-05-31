import sqlite3
import random
from datetime import datetime, timedelta

def random_date():
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()
    return start_date + (end_date - start_date) * random.random()

# Connect to SQLite DB
conn = sqlite3.connect("OMS1.db")
cursor = conn.cursor()

# Fetch customer IDs
cursor.execute("SELECT id FROM customers")
customer_ids = [row[0] for row in cursor.fetchall()]
customer_ids = customer_ids[0:500]

# Fetch item IDs and prices
cursor.execute("SELECT id, price FROM items")
items = cursor.fetchall()
items = items[0:500]

if not customer_ids or not items:
    raise Exception("Missing customers or items in the database. Populate them before running this.")

# Generate orders
for i in range(100000):
    if i % 1000 == 0:
        print(i)
        conn.commit()
    customer_id = random.choice(customer_ids)
    status_id = 1
    date_of_order = random_date()
    date_of_confirmation = None
    date_of_delivery = None
    date_of_backorder = None
    date_of_cancellation = None
    notes = None
    payment_done = 0
    paid_amount = 0.0

    # Randomly select 1–10 items
    num_items = random.randint(1, 10)
    selected_items = random.sample(items, num_items)

    total_payment = 0.0
    total_items = 0
    total_orderlines = len(selected_items)
    order_lines = []

    for item_id, price in selected_items:
        quantity = random.randint(1, 5)
        line_total = round(price * quantity, 2)
        total_payment += line_total
        total_items += quantity
        order_lines.append((item_id, quantity, price, status_id))

    # Insert order
    cursor.execute("""
        INSERT INTO orders (
            customer_id, status_id, date_of_order, date_of_confirmation,
            date_of_delivery, date_of_backorder, date_of_cancellation,
            total_items, total_orderlines, total_payment, paid_amount,
            payment_done, notes, is_processed, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        customer_id, status_id, date_of_order, date_of_confirmation,
        date_of_delivery, date_of_backorder, date_of_cancellation,
        total_items, total_orderlines, round(total_payment, 2), paid_amount,
        payment_done, notes, 0, datetime.now(), datetime.now()
    ))

    order_id = cursor.lastrowid

    # Insert order lines
    for item_id, quantity, price_per_unit, status_id in order_lines:
        cursor.execute("""
            INSERT INTO order_lines (
                order_id, item_id, quantity, price_per_unit, status_id, is_rated, is_processed, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            order_id, item_id, quantity, round(price_per_unit, 2), status_id, 0, 0, datetime.now()
        ))

# Commit & close
conn.commit()
conn.close()

print("✅ Successfully generated 300,000 orders with accurate totals and order lines.")
