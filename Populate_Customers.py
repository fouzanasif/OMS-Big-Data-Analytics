import sqlite3
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Connect to SQLite database
conn = sqlite3.connect('OMS.db')  # Replace with your database file
cursor = conn.cursor()

# Fetch all countries and cities
cursor.execute("SELECT id FROM countries")
country_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT id, country_id FROM cities")
cities = cursor.fetchall()

# Function to generate a random date within the last year
def random_date():
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()
    return start_date + (end_date - start_date) * random.random()

# Generate and insert customer data
for i in range(1000000):
    if i % 100000 == 0:
        print(i)
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.unique.email()
    phone = fake.phone_number()
    address_line1 = fake.street_address()
    address_line2 = fake.secondary_address()
    zipcode = fake.postcode() if random.choice([True, False]) else None
    active = random.choice([0, 1])
    last_activity = random_date()
    balance = round(random.uniform(0, 1000), 2)
    created_at = random_date()

    # 50% chance to assign both city and country, else only country
    if random.choice([True, False]) and cities:
        city_id, country_id = random.choice(cities)
    else:
        city_id = None
        country_id = random.choice(country_ids)

    # Insert into customers table
    cursor.execute("""
        INSERT INTO customers (
            first_name, last_name, email, phone, address_line1, address_line2,
            zipcode, city_id, country_id, active, last_activity, balance, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        first_name, last_name, email, phone, address_line1, address_line2,
        zipcode, city_id, country_id, active, last_activity, balance, created_at
    ))

# Commit changes and close connection
conn.commit()
conn.close()
