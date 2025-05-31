import sqlite3
import random
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
import itertools
from tqdm import tqdm

# Load static data once globally
def load_static_data():
    conn = sqlite3.connect("OMS.db")
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT id FROM items")
    item_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT id, name, status_code, description FROM events")
    events = cursor.fetchall()
    event_map = {event[1]: (event[0], event[2], event[3]) for event in events}

    conn.close()
    return customer_ids, item_ids, event_map


customer_ids, item_ids, event_map = load_static_data()
events_with_item = {'FOCUS', 'SCROLL', 'ADDED', 'REMOVED', 'CANCELLED', 'NOTFOUND'}
middle_event_pool = list(set(event_map.keys()) - {'LOGIN', 'LOGOUT'})


# Random timestamp generator
def random_datetime():
    return datetime.now() - timedelta(days=random.randint(0, 1), hours=random.randint(0, 23), minutes=random.randint(0, 59))


# Generate N user activity rows with tqdm progress
def generate_logs(n):
    logs_batch = []
    for _ in tqdm(range(n), desc="Generating logs", position=0, leave=False):
        customer_id = random.choice(customer_ids)
        logs = ['LOGIN']
        num_middle = random.randint(4, 20)
        logs += random.choices(middle_event_pool, k=num_middle)
        logs.append('LOGOUT')

        for event_name in logs:
            event_id, status_code, description = event_map[event_name]
            item_id = random.choice(item_ids) if event_name in events_with_item else None
            is_processed = random.choice([0, 1])
            event_time = random_datetime()
            logs_batch.append((
                customer_id, event_id, status_code, description,
                item_id, event_time, is_processed, event_time
            ))
    return logs_batch


# Insert into DB with tqdm progress
def insert_logs(logs):
    conn = sqlite3.connect("OMS.db")
    cursor = conn.cursor()
    total = len(logs)

    # Insert in chunks with progress bar
    for i in tqdm(range(0, total, 5000), desc="Inserting logs", position=1):
        chunk = logs[i:i+5000]
        cursor.executemany("""
            INSERT INTO user_activity (
                customer_id, event_id, status_code, description,
                item_id, datetime, is_processed, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, chunk)
        conn.commit()

    conn.close()


# Main driver
if __name__ == "__main__":
    TOTAL_LOGS = 100_000
    NUM_WORKERS = 4
    BATCH_SIZE = TOTAL_LOGS // NUM_WORKERS

    print("🚀 Generating logs in parallel...")

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(generate_logs, BATCH_SIZE) for _ in range(NUM_WORKERS)]
        all_logs = list(itertools.chain.from_iterable(f.result() for f in tqdm(futures, desc="Collecting results", position=2)))

    print(f"✅ Generated {len(all_logs)} logs. Inserting into DB...")

    insert_logs(all_logs)

    print("🎉 Done. Inserted 100,000 user activity logs.")



# import sqlite3
# import random
# from datetime import datetime, timedelta

# # Connect to SQLite DB
# conn = sqlite3.connect("OMS.db")
# cursor = conn.cursor()

# # Load customers
# cursor.execute("SELECT id FROM customers")
# customer_ids = [row[0] for row in cursor.fetchall()]

# # Load items
# cursor.execute("SELECT id FROM items")
# item_ids = [row[0] for row in cursor.fetchall()]

# # Load events and map by name
# cursor.execute("SELECT id, name, status_code, description FROM events")
# events = cursor.fetchall()
# event_map = {event[1]: (event[0], event[2], event[3]) for event in events}

# # Events that require item_id
# events_with_item = {'FOCUS', 'SCROLL', 'ADDED', 'REMOVED', 'CANCELLED', 'NOTFOUND'}
# events_without_item = set(event_map.keys()) - events_with_item

# # Valid events (excluding LOGIN/LOGOUT for middle events)
# middle_event_pool = list(set(event_map.keys()) - {'LOGIN', 'LOGOUT'})

# # Helper to generate random datetime in past 60 days
# def random_datetime():
#     return datetime.now() - timedelta(days=random.randint(0, 1), hours=random.randint(0, 23), minutes=random.randint(0, 59))

# # Create 100,000 user activity logs
# x = 0
# for _ in range(100000):
#     x+=1
#     print(x)
#     customer_id = random.choice(customer_ids)

#     # Start with LOGIN
#     logs = ['LOGIN']
#     num_middle = random.randint(4, 20)
#     logs += random.choices(middle_event_pool, k=num_middle)
#     logs.append('LOGOUT')
    
#     for event_name in logs:
#         event_id, status_code, description = event_map[event_name]
#         item_id = random.choice(item_ids) if event_name in events_with_item else None
#         is_processed = random.choice([0, 1])
#         event_time = random_datetime()

#         cursor.execute("""
#             INSERT INTO user_activity (
#                 customer_id, event_id, status_code, description,
#                 item_id, datetime, is_processed, created_at
#             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#         """, (
#             customer_id, event_id, status_code, description,
#             item_id, event_time, is_processed, event_time
#         ))
#     conn.commit()

# # Finalize
# conn.commit()
# conn.close()

# print("✅ Successfully inserted 100,000 user activity logs.")
