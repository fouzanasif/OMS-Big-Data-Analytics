from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
import os

# === CONFIGURATION ===
DB_PATH = "OMS.db"
JDBC_JAR = "sqlite-jdbc-3.49.1.0.jar"
CHECKPOINT_DIR = "analytics_checkpoints"
OUTPUT_DIR = "analytics_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


spark = SparkSession.builder \
    .appName("OrderAnalytics") \
    .master("local[6]") \
    .config("spark.executor.cores", "6") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.jars", JDBC_JAR) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()
    
# === LOAD TABLES FROM SQLITE ===
tables = ['Orders', 'Order_lines', 'Customers', 'Vendors', 'Items', 'cities', 'countries', "item_types"]
for table in tables:
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", table) \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    df.createOrReplaceTempView(table)

# main_query = """
#     WITH FilteredOrders AS (
#         SELECT O.ID, O.Customer_ID
#         FROM Orders O
#         WHERE O.IS_PROCESSED = 0
#     ),
#     FilteredOrderLines AS (
#         SELECT OL.Order_ID, OL.Item_ID, OL.Quantity
#         FROM Order_Lines OL
#         JOIN FilteredOrders FO ON OL.Order_ID = FO.ID
#     )
#     SELECT O.ID AS Order_ID, O.Customer_ID, C.Country_ID, C.City_ID, CO.Name as Country, CI.Name as City,
#            OL.Item_ID, OL.Quantity, I.Vendor_ID, V.Name AS Vendor_Name, I.Name as Item_Name, IT.name AS Item_Type
#     FROM Orders O
#     JOIN Customers C ON O.Customer_ID = C.ID
#     JOIN cITIES CI ON C.City_ID = CI.ID
#     JOIN Countries CO ON C.Country_ID = CO.ID
#     JOIN FilteredOrderLines OL ON O.ID = OL.Order_ID
#     JOIN Items I ON OL.Item_ID = I.ID
#     JOIN Item_types IT ON I.Item_type_ID = IT.ID
#     JOIN Vendors V ON I.Vendor_ID = V.ID
#     LIMIT 1000
# """
# df = spark.sql(main_query)
# df.createOrReplaceTempView("OrderData")

analytics_queries = {
    # Customer Analytics
    "cumulative_customers_monthly": """
        WITH monthly_customers AS (
            SELECT 
                substr(date_of_order, 6, 2) AS month,
                COUNT(DISTINCT customer_id) AS new_customers
            FROM Orders
            JOIN Customers ON Orders.customer_id = Customers.id
            GROUP BY month
        )
        SELECT 
            month,
            SUM(new_customers) OVER (ORDER BY month) AS cumulative_customers
        FROM monthly_customers
        ORDER BY month
    """,
    
    "top_loyal_customers": """
        SELECT 
            C.id AS customer_id,
            C.first_name || ' ' || C.last_name AS customer_name,
            COUNT(DISTINCT O.id) AS order_count,
            SUM(O.total_payment) AS total_spent
        FROM Customers C
        JOIN Orders O ON C.id = O.customer_id
        GROUP BY C.id, customer_name
        ORDER BY order_count DESC
        LIMIT 10
    """,
    
    "customer_segments": """
        WITH customer_order_counts AS (
        SELECT 
            C.id AS customer_id,
            COUNT(DISTINCT O.id) AS order_count
        FROM Customers C
        LEFT JOIN Orders O ON C.id = O.customer_id
        GROUP BY C.id
    )
        SELECT 
            CASE 
                WHEN order_count > 10 THEN 'Premium'
                WHEN order_count > 5 THEN 'Regular'
                ELSE 'Casual'
            END AS segment,
            COUNT(DISTINCT customer_id) AS customer_count
        FROM customer_order_counts
        GROUP BY segment
    """,
    
    # Vendor Analytics
    "top_selling_vendors": """
        SELECT 
            V.id AS vendor_id,
            V.name AS vendor_name,
            SUM(OL.quantity) AS items_sold,
            SUM(OL.total_price) AS revenue
        FROM Vendors V
        JOIN Items I ON V.id = I.vendor_id
        JOIN Order_lines OL ON I.id = OL.item_id
        GROUP BY V.id, V.name
        ORDER BY revenue DESC
        LIMIT 10
    """,
    
    # Items Analytics
    "top_10_items": """
        SELECT 
            I.id AS item_id,
            I.name AS item_name,
            COUNT(DISTINCT OL.order_id) AS order_count
        FROM Items I
        JOIN Order_lines OL ON I.id = OL.item_id
        GROUP BY I.id, I.name
        ORDER BY order_count DESC
        LIMIT 10
    """,
    
    # Orders Analytics
    "monthly_order_estimates": """
        SELECT 
            substr(date_of_order, 6, 2) AS month,
            COUNT(DISTINCT id) AS order_count
        FROM Orders
        GROUP BY month
        ORDER BY month
    """
}

results = {}

for key, query in analytics_queries.items():
    parquet_dir = os.path.join(OUTPUT_DIR, f"{key}.parquet")
    
    # Checkpoint logic
    if os.path.exists(parquet_dir) and any(f.startswith("part-") for f in os.listdir(parquet_dir)):
        print(f"📂 Loading cached data for {key}")
        df = spark.read.parquet(parquet_dir)
    else:
        print(f"⚙️ Executing query for {key}")
        df = spark.sql(query)
        df.write.mode("overwrite").parquet(parquet_dir)
        print(f"✅ Saved Parquet for {key}")
    
    pdf = df.toPandas()
    results[key] = pdf
    
    # Save outputs
    pdf.to_json(f"{OUTPUT_DIR}/{key}.json", orient="records", lines=True)
    pdf.to_csv(f"{OUTPUT_DIR}/{key}.csv", index=False)
    
    # Generate visualizations
    if key == "cumulative_customers_monthly":
        fig = px.line(pdf, x='month', y='cumulative_customers', 
                     title='Cumulative Customers by Month')
        fig.write_image(f"{OUTPUT_DIR}/{key}_viz.png")
        
    elif key == "top_loyal_customers":
        # Save as table
        pdf.to_csv(f"{OUTPUT_DIR}/top_loyal_customers_table.csv", index=False)
        fig = px.bar(pdf, x='customer_name', y='order_count',
                    title='Top 10 Loyal Customers')
        fig.write_image(f"{OUTPUT_DIR}/{key}_viz.png")
        
    elif key == "customer_segments":
        fig = px.pie(pdf, values='customer_count', names='segment',
                    title='Customer Segmentation')
        fig.write_image(f"{OUTPUT_DIR}/{key}_viz.png")
        
    elif key == "top_selling_vendors":
        fig = px.bar(pdf, x='vendor_name', y='revenue',
                    title='Top Selling Vendors by Revenue')
        fig.write_image(f"{OUTPUT_DIR}/{key}_viz.png")
        
    elif key == "top_10_items":
        fig = px.line(pdf, x='item_name', y='order_count',
                     title='Top 10 Items by Order Count')
        fig.write_image(f"{OUTPUT_DIR}/{key}_viz.png")
        
    elif key == "monthly_order_estimates":
        fig = px.bar(pdf, x='month', y='order_count',
                    title='Monthly Order Estimates')
        fig.write_image(f"{OUTPUT_DIR}/{key}_viz.png")
    
    print(f"✅ Completed processing for {key}")

# Customers per region (Choropleth)
query = """
SELECT 
    CO.name AS country,
    COUNT(DISTINCT C.id) AS customer_count
FROM Customers C
JOIN countries CO ON C.country_id = CO.id
GROUP BY CO.name
"""
pdf = spark.sql(query).toPandas()
fig = px.choropleth(pdf, locations='country', locationmode='country names',
                   color='customer_count', title='Customers by Country')
fig.write_image(f"{OUTPUT_DIR}/customers_per_region_viz.png")

# Active vs less active customers
query = """
SELECT 
    CASE 
        WHEN last_activity >= add_months(current_date(), -3) THEN 'Active'
        WHEN last_activity >= add_months(current_date(), -12) THEN 'Less Active'
        ELSE 'Inactive'
    END AS activity_status,
    COUNT(*) AS customer_count
FROM Customers
GROUP BY 1
"""
pdf = spark.sql(query).toPandas()
fig = px.bar(pdf, x='activity_status', y='customer_count', 
            title='Customer Activity Status')
fig.write_image(f"{OUTPUT_DIR}/customer_activity_viz.png")

# Vendor profits per month (Line graph)
query = """
SELECT 
    V.name AS vendor_name,
    substr(date_of_order, 6, 2) AS month,
    SUM(OL.total_price) AS revenue
FROM Vendors V
JOIN Items I ON V.id = I.vendor_id
JOIN Order_lines OL ON I.id = OL.item_id
JOIN Orders O ON OL.order_id = O.id
GROUP BY V.name, month
ORDER BY revenue DESC
LIMIT 10
"""
pdf = spark.sql(query).toPandas()
fig = px.line(pdf, x='month', y='revenue', color='vendor_name',
             title='Top 10 Vendor Profits by Month')
fig.write_image(f"{OUTPUT_DIR}/vendor_profits_monthly_viz.png")

# Top 3 items for top 10 vendors
query = """
WITH top_vendors AS (
    SELECT V.id FROM Vendors V
    JOIN Items I ON V.id = I.vendor_id
    JOIN Order_lines OL ON I.id = OL.item_id
    GROUP BY V.id
    ORDER BY SUM(OL.quantity) DESC
    LIMIT 10
)
SELECT 
    V.name AS vendor_name,
    I.name AS item_name,
    SUM(OL.quantity) AS quantity_sold
FROM Vendors V
JOIN Items I ON V.id = I.vendor_id
JOIN Order_lines OL ON I.id = OL.item_id
WHERE V.id IN (SELECT id FROM top_vendors)
GROUP BY V.name, I.name
ORDER BY quantity_sold DESC
LIMIT 30
"""
pdf = spark.sql(query).toPandas()
fig = px.bar(pdf, x='quantity_sold', y='item_name', color='vendor_name',
            orientation='h', title='Top 3 Items per Top Vendor')
fig.write_image(f"{OUTPUT_DIR}/top_items_per_vendor_viz.png")

# Top item types by region (Subplots)
query = """
SELECT 
    CO.name AS country,
    IT.name AS item_type,
    COUNT(*) AS order_count
FROM Order_lines OL
JOIN Orders O ON OL.order_id = O.id
JOIN Customers C ON O.customer_id = C.id
JOIN countries CO ON C.country_id = CO.id
JOIN Items I ON OL.item_id = I.id
JOIN item_types IT ON I.item_type_id = IT.id
GROUP BY CO.name, IT.name
ORDER BY order_count DESC
LIMIT 6
"""
pdf = spark.sql(query).toPandas()
fig = px.choropleth(pdf, locations='country', locationmode='country names',
                   color='order_count', facet_col='item_type',
                   title='Top Item Types by Region')
fig.write_image(f"{OUTPUT_DIR}/item_types_by_region_viz.png")

# Top 3 item types by month
query = """
SELECT 
    substr(date_of_order, 6, 2) AS month,
    IT.name AS item_type,
    COUNT(*) AS order_count
FROM Order_lines OL
JOIN Orders O ON OL.order_id = O.id
JOIN Items I ON OL.item_id = I.id
JOIN item_types IT ON I.item_type_id = IT.id
GROUP BY month, item_type
ORDER BY order_count DESC
LIMIT 36
"""
pdf = spark.sql(query).toPandas()
fig = px.bar(pdf, x='month', y='order_count', color='item_type',
            title='Top 3 Item Types by Month')
fig.write_image(f"{OUTPUT_DIR}/item_types_monthly_viz.png")

# Monthly sales estimate
query = """
SELECT 
    substr(date_of_order, 6, 2) AS month,
    SUM(total_payment) AS total_sales
FROM Orders
GROUP BY month
ORDER BY month
"""
pdf = spark.sql(query).toPandas()
fig = px.bar(pdf, x='month', y='total_sales', title='Monthly Sales Estimate')
fig.write_image(f"{OUTPUT_DIR}/monthly_sales_viz.png")

# Orders per region (Choropleth)
query = """
SELECT 
    CO.name AS country,
    COUNT(*) AS order_count
FROM Orders O
JOIN Customers C ON O.customer_id = C.id
JOIN countries CO ON C.country_id = CO.id
GROUP BY CO.name
"""
pdf = spark.sql(query).toPandas()
fig = px.choropleth(pdf, locations='country', locationmode='country names',
                   color='order_count', title='Orders by Country')
fig.write_image(f"{OUTPUT_DIR}/orders_per_region_viz.png")