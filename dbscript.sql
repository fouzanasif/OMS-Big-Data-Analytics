-- Order Management System Database Schema
-- SQLite Compatible Script

-- Enable foreign key constraints (SQLite specific)
PRAGMA foreign_keys = ON;

-- Drop existing tables if they exist (for clean recreation)
DROP TABLE IF EXISTS order_lines;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS vendors;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS cities;
DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS order_status;

-- 1. Countries Table
CREATE TABLE countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    country_code TEXT UNIQUE, -- ISO 2-letter country code
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. Cities Table
CREATE TABLE cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    country_id INTEGER NOT NULL,
    state_province TEXT, -- State or Province name
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE RESTRICT,
    UNIQUE(name, country_id)
);

-- 3. Order Status Table
CREATE TABLE order_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status_name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- 4. Vendors Table
CREATE TABLE vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    phone TEXT,
    active INTEGER DEFAULT 1 CHECK (active IN (0,1))
);

-- 5. Customers Table
CREATE TABLE customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    zipcode TEXT CHECK (
        (zipcode GLOB '[0-9][0-9][0-9][0-9][0-9]') OR zipcode IS NULL
    ),
    city_id INTEGER,
    country_id INTEGER NOT NULL,
    active INTEGER DEFAULT 1 CHECK (active IN (0,1)),
    last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
    balance REAL DEFAULT 0.00 CHECK (balance >= 0),
	rating REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (city_id) REFERENCES cities(id) ON DELETE SET NULL,
    FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE RESTRICT
);


-- Create indexes for customers
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_last_activity ON customers(last_activity);

-- 6. Items Table
CREATE TABLE item_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

-- Updated Table: Items
CREATE TABLE items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT UNIQUE,
    name TEXT NOT NULL,
    item_type_id INTEGER, -- References item_types
    color TEXT,
    size TEXT,
    price REAL NOT NULL CHECK (price >= 0),
    vendor_id INTEGER,
    quantity_in_stock INTEGER NOT NULL DEFAULT 0 CHECK (quantity_in_stock >= 0),
    rating REAL CHECK (rating >= 0 AND rating <= 5),
    times_purchased INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1 CHECK (active IN (0,1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vendor_id) REFERENCES vendors(id) ON DELETE SET NULL,
    FOREIGN KEY (item_type_id) REFERENCES item_types(id) ON DELETE SET NULL
);

-- Create indexes for items
CREATE INDEX idx_items_name ON items(name);
CREATE INDEX idx_items_type ON items(item_type_id);
CREATE INDEX idx_items_price ON items(price);

-- 7. Orders Table
CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL DEFAULT 1,
    date_of_order DATETIME DEFAULT CURRENT_TIMESTAMP,
    date_of_confirmation DATETIME,
    date_of_delivery DATETIME,
    date_of_backorder DATETIME,
    date_of_cancellation DATETIME,
	total_items INTEGER,
	total_orderlines INTEGER,
    total_payment REAL NOT NULL DEFAULT 0.00,
    paid_amount REAL DEFAULT 0.00,
    payment_done INTEGER DEFAULT 0 CHECK (payment_done IN (0,1)),
    notes TEXT,
	is_processed INTEGER DEFAULT 0 CHECK (is_processed IN (0,1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE RESTRICT,
    FOREIGN KEY (status_id) REFERENCES order_status(id) ON DELETE RESTRICT
);

-- Create indexes for orders
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status_id);
CREATE INDEX idx_orders_date ON orders(date_of_order);

-- 8. Order Lines Table
CREATE TABLE order_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_per_unit REAL NOT NULL CHECK (price_per_unit >= 0),
    total_price REAL GENERATED ALWAYS AS (quantity * price_per_unit) STORED,
    status_id INTEGER NOT NULL DEFAULT 1,
	is_rated INTEGER DEFAULT 0 CHECK (is_rated IN (0,1)),
	is_processed INTEGER DEFAULT 0 CHECK (is_processed IN (0,1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE RESTRICT,
    FOREIGN KEY (status_id) REFERENCES order_status(id) ON DELETE RESTRICT,
    UNIQUE(order_id, item_id)
);

-- Create indexes for order_lines
CREATE INDEX idx_order_lines_order ON order_lines(order_id);
CREATE INDEX idx_order_lines_item ON order_lines(item_id);

INSERT INTO order_status (STATUS_NAME, DESCRIPTION) VALUES 
('PLACED', 'The Order has been recorded'),
('CONFIRMED', 'The Order has been confirmed'),
('DELIVERED', 'The Order has been delivered'),
('CANCELLED', 'The Order has been cancelled'),
('BACKORDERED', 'The Order has been back ordered'),
('RETURNED', 'The Order has been returned'),
('REVERSED', 'The Order could not be delivered'),
('CANCELLATION_REQUESTED', 'The return request has been initiated'),
('RETURN_REQUESTED', 'The return request has been initiated');

-- Insert Sample Vendors
/*INSERT INTO vendors (name, email, phone) VALUES
('Tech Solutions Inc', 'contact@techsolutions.com', '+1-555-0101'),
('Fashion World Ltd', 'info@fashionworld.com', '+1-555-0102'),
('Home & Garden Co', 'sales@homeandgarden.com', '+1-555-0103');

-- Insert Sample Customers
INSERT INTO customers (first_name, last_name, email, phone, address_line1, zipcode, city_id) VALUES
('John', 'Doe', 'john.doe@email.com', '+1-555-1001', '123 Main St', '10001', 1),
('Jane', 'Smith', 'jane.smith@email.com', '+1-555-1002', '456 Oak Ave', '90210', 2),
('Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-1003', '789 Pine Rd', '10002', 1);

-- Insert Sample Items
INSERT INTO items (name, type, color, size, price, vendor_id, quantity_in_stock, rating) VALUES
('Wireless Headphones', 'Electronics', 'Black', 'One Size', 99.99, 1, 50, 4.5),
('Cotton T-Shirt', 'Clothing', 'Blue', 'Medium', 19.99, 2, 100, 4.2),
('Garden Shovel', 'Tools', 'Brown', 'Standard', 29.99, 3, 25, 4.0),
('Smartphone Case', 'Electronics', 'Clear', 'iPhone 14', 15.99, 1, 200, 3.8),
('Summer Dress', 'Clothing', 'Red', 'Large', 49.99, 2, 30, 4.7);
*/
-- Create Triggers for maintaining data integrity

-- Trigger to update order total when order lines change
CREATE TRIGGER update_order_total_after_insert
    AFTER INSERT ON order_lines
BEGIN
    UPDATE orders 
    SET total_payment = (
        SELECT COALESCE(SUM(total_price), 0) 
        FROM order_lines 
        WHERE order_id = NEW.order_id
    ),
    updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.order_id;
END;

CREATE TRIGGER update_order_total_after_update
    AFTER UPDATE ON order_lines
BEGIN
    UPDATE orders 
    SET total_payment = (
        SELECT COALESCE(SUM(total_price), 0) 
        FROM order_lines 
        WHERE order_id = NEW.order_id
    ),
    updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.order_id;
END;

CREATE TRIGGER update_order_total_after_delete
    AFTER DELETE ON order_lines
BEGIN
    UPDATE orders 
    SET total_payment = (
        SELECT COALESCE(SUM(total_price), 0) 
        FROM order_lines 
        WHERE order_id = OLD.order_id
    ),
    updated_at = CURRENT_TIMESTAMP
    WHERE id = OLD.order_id;
END;

-- Trigger to update item times_purchased when order is delivered
CREATE TRIGGER update_item_purchase_count
    AFTER UPDATE ON orders
    WHEN NEW.status_id = 5 AND OLD.status_id != 5 -- Status changed to 'Delivered'
BEGIN
    UPDATE items 
    SET times_purchased = times_purchased + (
        SELECT COALESCE(SUM(ol.quantity), 0)
        FROM order_lines ol 
        WHERE ol.order_id = NEW.id AND ol.item_id = items.id
    )
    WHERE id IN (
        SELECT item_id 
        FROM order_lines 
        WHERE order_id = NEW.id
    );
END;


-- 9. Events Table (for user activity tracking)
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    status_code TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, status_code)
);

-- 10. User Activity Table (Activity Logs)
CREATE TABLE user_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    status_code TEXT NOT NULL,
    description TEXT,
    item_id INTEGER,
    datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_processed INTEGER DEFAULT 0 CHECK (is_processed IN (0,1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE RESTRICT,
    FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE SET NULL,
    FOREIGN KEY (event_id, status_code) REFERENCES events(id, status_code) ON DELETE RESTRICT
);

-- Create indexes for user_activity
CREATE INDEX idx_user_activity_customer ON user_activity(customer_id);
CREATE INDEX idx_user_activity_event ON user_activity(event_id);
CREATE INDEX idx_user_activity_datetime ON user_activity(datetime);
CREATE INDEX idx_user_activity_item ON user_activity(item_id);

CREATE TABLE Ratings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    order_line_id INTEGER NOT NULL UNIQUE,
    rating REAL NOT NULL CHECK (rating >= 0 AND rating <= 5),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE,
    FOREIGN KEY (order_line_id) REFERENCES order_lines(id) ON DELETE CASCADE
);


-- Additional Trigger to update customer last_activity from user_activity
CREATE TRIGGER update_customer_activity_from_logs
    AFTER INSERT ON user_activity
BEGIN
    UPDATE customers 
    SET last_activity = NEW.datetime
    WHERE id = NEW.customer_id;
END;

CREATE TABLE Order_Updates (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    OrderID INTEGER NOT NULL,
    Last_status INTEGER,
    Current_status INTEGER NOT NULL,
    Date_last_status DATETIME,
    Date_next_status DATETIME,
    is_processed INTEGER DEFAULT 0 CHECK (is_processed IN (0,1)),
    FOREIGN KEY (OrderID) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (Last_status) REFERENCES order_status(id) ON DELETE SET NULL,
    FOREIGN KEY (Current_status) REFERENCES order_status(id) ON DELETE RESTRICT
);


