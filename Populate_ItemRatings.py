import sqlite3

# Path to your SQLite DB
db_path = "OMS.db"

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Step 1: Get average ratings per item_id from Ratings table
cursor.execute("""
    SELECT item_id, 
       AVG(rating) AS avg_rating, 
       COUNT(*) AS rating_count
    FROM Ratings
    GROUP BY item_id;

""")

# Step 2: Update the ratings column in the items table
for item_id, avg_rating, rating_count in cursor.fetchall():
    cursor.execute("""
        UPDATE items
        SET rating = ?, times_purchased = ?
        WHERE id = ?
    """, (round(avg_rating, 2), rating_count, item_id))

# Commit changes and close the connection
conn.commit()
conn.close()
