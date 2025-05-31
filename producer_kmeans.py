#!/usr/bin/env python3
"""
Producer.py - Intelligent Data Streaming for Order Management Analytics
Streams customer, item, and order data for real-time KMeans clustering
"""

import sqlite3
import socket
import json
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import random
OFFSET = 0
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderManagementProducer:
    def __init__(self, db_path: str = "OMS.db", host: str = "localhost", port: int = 9999):
        self.db_path = db_path
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        # self.scenarios = ["customer_segmentation", "inventory_intelligence", "order_patterns"]
        self.scenarios = ["customer_segmentation"]
        self.current_scenario = 0
        
        # Initialize database with sample data if needed
        self.init_database()
        
    def init_database(self):
        """Initialize database with sample data for testing"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if data exists
            cursor.execute("SELECT COUNT(*) FROM customers limit 10")
            if cursor.fetchone()[0] == 0:
                logger.info("Initializing database with sample data...")
                self._create_sample_data(conn)
                
            conn.close()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            
    def _create_sample_data(self, conn):
        """Create sample data for testing"""
        cursor = conn.cursor()
        
        # Sample countries
        countries_data = [
            ("USA", "US"), ("Canada", "CA"), ("UK", "GB"), 
            ("Germany", "DE"), ("Australia", "AU")
        ]
        cursor.executemany("INSERT INTO countries (name, country_code) VALUES (?, ?)", countries_data)
        
        # Sample cities
        cities_data = [
            ("New York", 1, "NY"), ("Los Angeles", 1, "CA"), ("Toronto", 2, "ON"),
            ("London", 3, ""), ("Berlin", 4, ""), ("Sydney", 5, "NSW")
        ]
        cursor.executemany("INSERT INTO cities (name, country_id, state_province) VALUES (?, ?, ?)", cities_data)
        
        # Sample order status
        status_data = [
            ("Pending", "Order placed, awaiting confirmation"),
            ("Confirmed", "Order confirmed, processing"),
            ("Shipped", "Order shipped"),
            ("Delivered", "Order delivered"),
            ("Cancelled", "Order cancelled")
        ]
        cursor.executemany("INSERT INTO order_status (status_name, description) VALUES (?, ?)", status_data)
        
        # Sample item types
        item_types = [("Electronics",), ("Clothing",), ("Books",), ("Home & Garden",), ("Sports",)]
        cursor.executemany("INSERT INTO item_types (name) VALUES (?)", item_types)
        
        # Sample vendors
        vendors_data = [
            ("TechCorp", "tech@corp.com", "555-0001", 1),
            ("FashionHub", "info@fashion.com", "555-0002", 1),
            ("BookWorld", "orders@books.com", "555-0003", 1)
        ]
        cursor.executemany("INSERT INTO vendors (name, email, phone, active) VALUES (?, ?, ?, ?)", vendors_data)
        
        # Generate sample customers (500 customers)
        customers_data = []
        for i in range(500):
            first_name = f"Customer{i}"
            last_name = f"Lastname{i}"
            email = f"customer{i}@email.com"
            phone = f"555-{1000+i:04d}"
            city_id = random.randint(1, 6)
            country_id = random.randint(1, 5)
            balance = round(random.uniform(0, 1000), 2)
            rating = round(random.uniform(3.0, 5.0), 1)
            
            customers_data.append((first_name, last_name, email, phone, 
                                 f"Address {i}", "", f"{10000+i:05d}", 
                                 city_id, country_id, 1, 
                                 datetime.now(), balance, rating))
        
        cursor.executemany("""INSERT INTO customers 
                            (first_name, last_name, email, phone, address_line1, 
                             address_line2, zipcode, city_id, country_id, active, 
                             last_activity, balance, rating) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", customers_data)
        
        # Generate sample items (200 items)
        items_data = []
        for i in range(200):
            sku = f"SKU-{1000+i:04d}"
            name = f"Product {i}"
            item_type_id = random.randint(1, 5)
            price = round(random.uniform(10, 500), 2)
            vendor_id = random.randint(1, 3)
            stock = random.randint(0, 100)
            rating = round(random.uniform(3.0, 5.0), 1)
            times_purchased = random.randint(0, 200)
            
            items_data.append((sku, name, item_type_id, "Mixed", "One Size", 
                             price, vendor_id, stock, rating, times_purchased, 1))
        
        cursor.executemany("""INSERT INTO items 
                            (sku, name, item_type_id, color, size, price, 
                             vendor_id, quantity_in_stock, rating, times_purchased, active) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", items_data)
        
        # Generate sample orders (1000 orders)
        orders_data = []
        for i in range(1000):
            customer_id = random.randint(1, 500)
            status_id = random.randint(1, 5)
            total_payment = round(random.uniform(20, 1000), 2)
            paid_amount = total_payment if status_id >= 2 else round(random.uniform(0, total_payment), 2)
            payment_done = 1 if paid_amount >= total_payment else 0
            total_items = random.randint(1, 10)
            total_orderlines = random.randint(1, 5)
            
            orders_data.append((customer_id, status_id, total_items, total_orderlines,
                              total_payment, paid_amount, payment_done, 0))
        
        cursor.executemany("""INSERT INTO orders 
                            (customer_id, status_id, total_items, total_orderlines,
                             total_payment, paid_amount, payment_done, is_processed) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", orders_data)
        
        conn.commit()
        logger.info("Sample data created successfully")
    
    def get_customer_segmentation_data(self) -> List[Dict[str, Any]]:
        """Get customer data for segmentation analysis"""
        query = """
        SELECT 
            c.id as customer_id,
            c.balance,
            c.rating as customer_rating,
            COUNT(o.id) as total_orders,
            AVG(o.total_payment) as avg_order_value,
            SUM(o.total_payment) as total_spent,
            MAX(o.date_of_order) as last_order_date,
            COUNT(DISTINCT DATE(o.date_of_order)) as active_days,
            co.name as country,
            ci.name as city
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        LEFT JOIN countries co ON c.country_id = co.id
        LEFT JOIN cities ci ON c.city_id = ci.id
        WHERE c.active = 1
        GROUP BY c.id, c.balance, c.rating, co.name, ci.name
        HAVING total_orders > 0
        ORDER BY RANDOM()
        LIMIT 100
        OFFSET ?
        """
        
        return self._execute_query(query)
    
    def get_inventory_intelligence_data(self) -> List[Dict[str, Any]]:
        """Get item data for inventory optimization"""
        query = """
        SELECT 
            i.id as item_id,
            i.sku,
            i.name as item_name,
            i.price,
            i.quantity_in_stock,
            i.rating as item_rating,
            i.times_purchased,
            it.name as item_type,
            v.name as vendor_name,
            COUNT(ol.id) as recent_orders,
            AVG(ol.quantity) as avg_quantity_per_order,
            SUM(ol.quantity) as total_quantity_sold
        FROM items i
        LEFT JOIN item_types it ON i.item_type_id = it.id
        LEFT JOIN vendors v ON i.vendor_id = v.id
        LEFT JOIN order_lines ol ON i.id = ol.item_id
        LEFT JOIN orders o ON ol.order_id = o.id
        WHERE i.active = 1 
        AND (o.date_of_order IS NULL OR o.date_of_order >= datetime('now', '-30 days'))
        GROUP BY i.id, i.sku, i.name, i.price, i.quantity_in_stock, 
                 i.rating, i.times_purchased, it.name, v.name
        ORDER BY RANDOM()
        LIMIT 100
        """
        
        return self._execute_query(query)
    
    def get_order_patterns_data(self) -> List[Dict[str, Any]]:
        """Get order data for operational optimization"""
        query = """
        SELECT 
            o.id as order_id,
            o.total_items,
            o.total_orderlines,
            o.total_payment,
            o.paid_amount,
            o.payment_done,
            os.status_name,
            c.balance as customer_balance,
            c.rating as customer_rating,
            co.name as country,
            ci.name as city,
            julianday('now') - julianday(o.date_of_order) as days_since_order,
            COUNT(ol.id) as line_items_count,
            AVG(ol.price_per_unit) as avg_item_price
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN order_status os ON o.status_id = os.id
        LEFT JOIN countries co ON c.country_id = co.id
        LEFT JOIN cities ci ON c.city_id = ci.id
        LEFT JOIN order_lines ol ON o.id = ol.order_id
        WHERE o.date_of_order >= datetime('now', '-60 days')
        GROUP BY o.id, o.total_items, o.total_orderlines, o.total_payment,
                 o.paid_amount, o.payment_done, os.status_name,
                 c.balance, c.rating, co.name, ci.name, o.date_of_order
        ORDER BY RANDOM()
        LIMIT 1000
        OFFSET ?
        """
        
        return self._execute_query(query)
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        global OFFSET
        """Execute query and return results as list of dictionaries"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, (OFFSET,))
            OFFSET += 100
            results = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []
    
    def start_streaming(self):
        """Start streaming data to consumer"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(1)
            
            logger.info(f"Producer listening on {self.host}:{self.port}")
            self.running = True
            
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    logger.info(f"Connection from {address}")
                    
                    # Start streaming thread
                    stream_thread = threading.Thread(
                        target=self._stream_data, 
                        args=(client_socket,)
                    )
                    stream_thread.daemon = True
                    stream_thread.start()
                    
                except Exception as e:
                    logger.error(f"Connection error: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to start streaming: {e}")
        finally:
            self.cleanup()
    
    def _stream_data(self, client_socket):
        """Stream data continuously to client"""
        try:
            scenario_functions = {
                "customer_segmentation": self.get_customer_segmentation_data,
                # "inventory_intelligence": self.get_inventory_intelligence_data,
                # "order_patterns": self.get_order_patterns_data
            }
            
            while self.running:
                # Rotate through scenarios
                current_scenario = self.scenarios[self.current_scenario]
                print(current_scenario)
                data_function = scenario_functions[current_scenario]
                
                # Get data
                data = data_function()
                
                if data:
                    # Create message
                    message = {
                        "timestamp": datetime.now().isoformat(),
                        "scenario": current_scenario,
                        "batch_id": f"{current_scenario}_{int(time.time())}",
                        "data": data
                    }
                    
                    # Send data
                    json_message = json.dumps(message) + "\n"
                    print(json_message)
                    client_socket.send(json_message.encode('utf-8'))
                    
                    logger.info(f"Sent {len(data)} records for {current_scenario}")
                    
                    # Move to next scenario
                    self.current_scenario = (self.current_scenario + 1) % len(self.scenarios)
                    
                # Wait before next batch
                # time.sleep(10)  # 10 seconds between batches
                
                
        except Exception as e:
            logger.error(f"Streaming error: {e}")
        finally:
            client_socket.close()
    
    def cleanup(self):
        """Clean up resources"""
        self.running = False
        if self.socket:
            self.socket.close()
        logger.info("Producer cleaned up")

def main():
    """Main function to run the producer"""
    producer = OrderManagementProducer()
    
    try:
        producer.start_streaming()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.cleanup()

if __name__ == "__main__":
    main()