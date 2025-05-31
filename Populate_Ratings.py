import sqlite3
import random
from datetime import datetime
import numpy as np

conn = sqlite3.connect("OMS.db")
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(order_lines)")
columns = [col[1] for col in cursor.fetchall()]
if "is_rated" not in columns:
    cursor.execute("ALTER TABLE order_lines ADD COLUMN is_rated INTEGER DEFAULT 0")

cursor.execute("""
    SELECT 
        ol.id AS order_line_id,
        o.customer_id,
        ol.item_id
    FROM order_lines ol
    JOIN orders o ON o.id = ol.order_id
    WHERE ol.is_rated = 0
""")
orderlines = cursor.fetchall()

print(f"Processing {len(orderlines)} unrated order lines...")
seeds=np.arange(0, 100)
for order_line_id, customer_id, item_id in orderlines:
    # Random rating: 0.0, 0.5, ..., 5.0
    random.seed(random.choice(seeds))
    rating = round(random.uniform(0, 5), 2)
    
    # rating = round(random.uniform(0, 5), 1)
    # if rating % 0.5 != 0:
    #     rating = round(rating * 2) / 2.0

    cursor.execute("""
        INSERT INTO Ratings (customer_id, item_id, order_line_id, rating, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (customer_id, item_id, order_line_id, rating, datetime.now()))

    # Mark the orderline as rated
    cursor.execute("""
        UPDATE order_lines SET is_rated = 1 WHERE id = ?
    """, (order_line_id,))

# Commit and close
conn.commit()
conn.close()

print("✅ Ratings generated and is_rated flags updated.")
