from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import os
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# === CONFIGURATION ===
DB_PATH = "OMS.db"
JDBC_JAR = "sqlite-jdbc-3.49.1.0.jar"
CHECKPOINT_DIR = "checkpoints"
OUTPUT_DIR = "analytics_outputs_3"
VISUALIZATIONS_DIR = "visualizations"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)

# Initialize Spark Session (exactly as you did)
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
tables = ['Orders', 'Order_lines', 'Customers', 'Vendors', 'Items', 'cities', 'countries', "item_types", "Ratings", "order_status"]
for table in tables:
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", table) \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    df.createOrReplaceTempView(table)

# === MAIN QUERY - FETCH ALL RELEVANT DATA WITH FILTERS ===
main_query = """
    WITH 
    -- Calculate average order value
    OrderStats AS (
        SELECT AVG(total_payment) AS avg_order_value
        FROM Orders
        WHERE status_id IN (SELECT id FROM order_status WHERE status_name NOT IN ('Cancelled', 'Failed'))
    ),
    
    -- Filter for popular items (purchased > 50 times)
    PopularItems AS (
        SELECT id from items where times_purchased >= 50
    ),
    
    -- Filter for active customers (with > 5 orders)
    ActiveCustomers AS (
        SELECT customer_id
        FROM Orders
        GROUP BY customer_id
        HAVING COUNT(*) > 5
    ),
    
    -- Filter for active vendors
    ActiveVendors AS (
        SELECT id 
        FROM Vendors 
        WHERE active = 1
    ),
    
    -- Filter for meaningful orders (not 25% below average)
    MeaningfulOrders AS (
        SELECT O.id
        FROM Orders O, OrderStats OS
        WHERE O.total_payment >= (OS.avg_order_value * 0.75)
        AND O.status_id IN (SELECT id FROM order_status WHERE status_name NOT IN ('Cancelled', 'Failed'))
    )
    
    SELECT 
        O.ID AS order_id,
        O.customer_id,
        O.date_of_order,
        O.status_id,
        O.total_payment,
        O.paid_amount,
        C.first_name,
        C.last_name,
        C.email,
        C.active,
        C.balance,
        C.rating AS customer_rating,
        C.created_at AS customer_since,
        CI.name AS city,
        CO.name AS country,
        OL.id AS order_line_id,
        OL.quantity,
        OL.price_per_unit,
        OL.total_price,
        OL.status_id AS line_status,
        I.id AS item_id,
        I.name AS item_name,
        I.price AS item_price,
        I.quantity_in_stock,
        I.rating AS item_rating,
        I.times_purchased,
        IT.name AS item_type,
        V.id AS vendor_id,
        V.name AS vendor_name,
        V.active AS vendor_active,
        -- Rating information
        R.rating AS line_item_rating,
        R.created_at AS rating_date
    FROM Orders O
    JOIN Customers C ON O.customer_id = C.id
    JOIN Cities CI ON C.city_id = CI.id
    JOIN Countries CO ON C.country_id = CO.id
    JOIN Order_lines OL ON O.id = OL.order_id
    JOIN Items I ON OL.item_id = I.id
    JOIN Item_types IT ON I.item_type_id = IT.id
    JOIN Vendors V ON I.vendor_id = V.id
    LEFT JOIN Ratings R ON OL.id = R.order_line_id AND I.id = R.item_id
    WHERE 

        O.id IN (SELECT id FROM MeaningfulOrders)
        AND OL.item_id IN (SELECT id FROM PopularItems)
        AND O.customer_id IN (SELECT customer_id FROM ActiveCustomers)
        AND V.id IN (SELECT id FROM ActiveVendors)
"""
df = spark.sql(main_query)
df.createOrReplaceTempView("analytics_base")

# === ANALYTICS QUERIES ===
queries = {
    # Customer Analytics
    "total_customers_trend": """
        SELECT 
            substr(created_at, 6, 2) AS month,
            COUNT(*) AS customer_count,
            SUM(COUNT(*)) OVER (ORDER BY substr(created_at, 6, 2)) AS cumulative_count
        FROM Customers
        GROUP BY substr(created_at, 6, 2)
        ORDER BY month
    """,
    
    "most_loyal_customers": """
        SELECT 
            customer_id,
            first_name || ' ' || last_name AS customer_name,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(total_price) AS total_spent,
            MAX(date_of_order) AS last_order_date
        FROM analytics_base
        GROUP BY customer_id, first_name, last_name
        ORDER BY total_orders DESC
        LIMIT 10
    """,
    
    "customers_per_region": """
        SELECT 
            country,
            COUNT(DISTINCT customer_id) AS customer_count
        FROM analytics_base
        GROUP BY country
    """,
    
    "customer_activity_segments": """
        WITH customer_orders AS (
            SELECT 
                customer_id,
                COUNT(DISTINCT order_id) AS order_count
            FROM analytics_base
            GROUP BY customer_id
        )
        SELECT 
            CASE 
                WHEN order_count >= 10 THEN 'Highly Active'
                WHEN order_count >= 5 THEN 'Active'
                WHEN order_count >= 1 THEN 'Less Active'
                ELSE 'Inactive'
            END AS activity_segment,
            COUNT(*) AS customer_count
        FROM customer_orders
        GROUP BY activity_segment
    """,
    
    # Vendor Analytics
    "top_selling_vendors": """
        SELECT 
            vendor_id,
            vendor_name,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(quantity) AS total_items_sold,
            SUM(total_price) AS total_revenue
        FROM analytics_base
        GROUP BY vendor_id, vendor_name
        ORDER BY total_revenue DESC
    """,
    
    "vendor_monthly_profits": """
        SELECT 
            vendor_id,
            vendor_name,
            substr(date_of_order, 6, 2) AS month,
            SUM(total_price) AS monthly_revenue
        FROM analytics_base
        GROUP BY vendor_id, vendor_name, substr(date_of_order, 6, 2)
        HAVING vendor_id IN (
            SELECT vendor_id FROM analytics_base 
            GROUP BY vendor_id 
            ORDER BY SUM(total_price) DESC 
            LIMIT 10
        )
        ORDER BY month, vendor_name
    """,
    
    "top_vendors_top_items": """
        WITH top_vendors AS (
            SELECT vendor_id FROM analytics_base 
            GROUP BY vendor_id 
            ORDER BY SUM(total_price) DESC 
            LIMIT 10
        ),
        vendor_items AS (
            SELECT 
                vendor_id,
                vendor_name,
                item_name,
                SUM(quantity) AS total_quantity,
                ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY SUM(quantity) DESC) AS rank
            FROM analytics_base
            WHERE vendor_id IN (SELECT vendor_id FROM top_vendors)
            GROUP BY vendor_id, vendor_name, item_name
        )
        SELECT * FROM vendor_items WHERE rank <= 3
    """,
    
    "top_vendors_item_types": """
        WITH top_vendors AS (
            SELECT vendor_id FROM analytics_base 
            GROUP BY vendor_id 
            ORDER BY SUM(total_price) DESC 
            LIMIT 10
        ),
        vendor_item_types AS (
            SELECT 
                vendor_id,
                vendor_name,
                item_type,
                SUM(quantity) AS total_quantity,
                ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY SUM(quantity) DESC) AS rank
            FROM analytics_base
            WHERE vendor_id IN (SELECT vendor_id FROM top_vendors)
            GROUP BY vendor_id, vendor_name, item_type
        )
        SELECT * FROM vendor_item_types WHERE rank <= 3
    """,
    
    "vendor_country_distribution": """
        WITH top_vendors AS (
            SELECT vendor_id FROM analytics_base 
            GROUP BY vendor_id 
            ORDER BY SUM(total_price) DESC 
            LIMIT 5
        )
        SELECT 
            vendor_id,
            vendor_name,
            country,
            COUNT(DISTINCT order_id) AS order_count
        FROM analytics_base
        WHERE vendor_id IN (SELECT vendor_id FROM top_vendors)
        GROUP BY vendor_id, vendor_name, country
    """,
    
    "underperforming_vendors": """
        SELECT 
            vendor_id,
            vendor_name,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(quantity) AS items_sold,
            SUM(total_price) AS revenue
        FROM analytics_base
        GROUP BY vendor_id, vendor_name
        ORDER BY revenue ASC
        LIMIT 20
    """,
    
    # # Item Analytics
    # "top_items_by_orders": """
    #     SELECT 
    #         item_id,
    #         item_name,
    #         substr(date_of_order, 6, 2) AS month,
    #         COUNT(DISTINCT order_id) AS order_count
    #     FROM analytics_base
    #     GROUP BY item_id, item_name, substr(date_of_order, 6, 2)
    #     HAVING item_id IN (
    #         SELECT item_id FROM analytics_base 
    #         GROUP BY item_id 
    #         ORDER BY COUNT(DISTINCT order_id) DESC 
    #         LIMIT 10
    #     )
    #     ORDER BY month, item_name
    # """,
    
    # "top_item_types_by_orders": """
    #     SELECT 
    #         item_type,
    #         substr(date_of_order, 6, 2) AS month,
    #         COUNT(DISTINCT order_id) AS order_count
    #     FROM analytics_base
    #     GROUP BY item_type, substr(date_of_order, 6, 2)
    #     HAVING item_type IN (
    #         SELECT item_type FROM analytics_base 
    #         GROUP BY item_type 
    #         ORDER BY COUNT(DISTINCT order_id) DESC 
    #         LIMIT 10
    #     )
    #     ORDER BY month, item_type
    # """,
    
    # "top_item_types_by_region": """
    #     WITH ranked_item_types AS (
    #         SELECT 
    #             country,
    #             item_type,
    #             COUNT(DISTINCT order_id) AS order_count,
    #             ROW_NUMBER() OVER (PARTITION BY country ORDER BY COUNT(DISTINCT order_id) DESC) AS rank
    #         FROM analytics_base
    #         GROUP BY country, item_type
    #     )
    #     SELECT * FROM ranked_item_types WHERE rank <= 6
    # """,
    
    # "top_items_by_region": """
    #     WITH ranked_items AS (
    #         SELECT 
    #             country,
    #             item_id,
    #             item_name,
    #             COUNT(DISTINCT order_id) AS order_count,
    #             ROW_NUMBER() OVER (PARTITION BY country ORDER BY COUNT(DISTINCT order_id) DESC) AS rank
    #         FROM analytics_base
    #         GROUP BY country, item_id, item_name
    #     )
    #     SELECT * FROM ranked_items WHERE rank <= 10
    # """,
    
    # "top_item_types_ratings": """
    #     SELECT 
    #         item_type,
    #         AVG(line_item_rating) AS avg_rating,
    #         COUNT(*) AS rating_count
    #     FROM analytics_base
    #     WHERE line_item_rating IS NOT NULL
    #     GROUP BY item_type
    #     ORDER BY avg_rating DESC
    #     LIMIT 10
    # """,
    
    # "underperforming_items": """
    #     SELECT 
    #         item_id,
    #         item_name,
    #         item_type,
    #         COUNT(DISTINCT order_id) AS order_count,
    #         SUM(quantity) AS total_quantity,
    #         AVG(line_item_rating) AS avg_rating
    #     FROM analytics_base
    #     GROUP BY item_id, item_name, item_type
    #     ORDER BY order_count ASC, avg_rating ASC
    #     LIMIT 10
    # """,
    
    # "item_types_monthly_orders": """
    #     SELECT 
    #         item_type,
    #         substr(date_of_order, 6, 2) AS month,
    #         COUNT(DISTINCT order_id) AS order_count
    #     FROM analytics_base
    #     WHERE item_type IN (
    #         SELECT item_type FROM analytics_base 
    #         GROUP BY item_type 
    #         ORDER BY COUNT(DISTINCT order_id) DESC 
    #         LIMIT 3
    #     )
    #     GROUP BY item_type, substr(date_of_order, 6, 2)
    #     ORDER BY month, item_type
    # """,
    
    # # New Order Analytics
    # "monthly_order_counts": """
    #     SELECT 
    #         substr(date_of_order, 6, 2) AS month,
    #         COUNT(DISTINCT order_id) AS order_count
    #     FROM analytics_base
    #     GROUP BY substr(date_of_order, 6, 2)
    #     ORDER BY month
    # """,
    
    # "monthly_sales_estimate": """
    #     SELECT 
    #         substr(date_of_order, 6, 2) AS month,
    #         SUM(total_price) AS total_sales
    #     FROM analytics_base
    #     GROUP BY substr(date_of_order, 6, 2)
    #     ORDER BY month
    # """,
    
    # "orders_by_status": """
    #     SELECT 
    #         OS.status_name,
    #         COUNT(DISTINCT O.id) AS order_count
    #     FROM Orders O
    #     JOIN order_status OS ON O.status_id = OS.id
    #     GROUP BY OS.status_name
    # """,
    
    # "orders_per_region": """
    #     SELECT 
    #         country,
    #         COUNT(DISTINCT order_id) AS order_count
    #     FROM analytics_base
    #     GROUP BY country
    # """,
    
    # "profits_per_region": """
    #     SELECT 
    #         country,
    #         SUM(total_price) AS total_profit
    #     FROM analytics_base
    #     GROUP BY country
    # """
}

# === EXECUTE QUERIES WITH CHECKPOINTING ===
results = {}
for key, query in queries.items():
    parquet_dir = os.path.join(OUTPUT_DIR, f"{key}.parquet")
    
    if os.path.exists(parquet_dir) and any(f.startswith("part-") for f in os.listdir(parquet_dir)):
        print(f"📂 Loading cached data for {key}")
        df = spark.read.parquet(parquet_dir)
    else:
        print(f"⚙️ Executing query for {key}")
        df = spark.sql(query)
        df.write.mode("overwrite").parquet(parquet_dir)
        print(f"✅ Saved Parquet for {key}")
    
    results[key] = df.toPandas()
    results[key].to_json(f"{OUTPUT_DIR}/{key}.json", orient="records", lines=True)
    results[key].to_csv(f"{OUTPUT_DIR}/{key}.csv", index=False)

# === VISUALIZATIONS ===
# Customer Visualizations
# 1. Total Customers Trend (Line Graph)
plt.figure(figsize=(12, 6))
sns.lineplot(data=results['total_customers_trend'], x='month', y='cumulative_count')
plt.title('Cumulative Customer Growth')
plt.xlabel('Month')
plt.ylabel('Total Customers')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(f"{VISUALIZATIONS_DIR}/customer_growth_trend.png")
plt.close()

# 2. Most Loyal Customers (Table)
results['most_loyal_customers'].to_csv(f"{VISUALIZATIONS_DIR}/top_loyal_customers.csv", index=False)

# 3. Customer Segments (Pie Chart)
plt.figure(figsize=(8, 8))
plt.pie(results['customer_activity_segments']['customer_count'], 
        labels=results['customer_activity_segments']['activity_segment'],
        autopct='%1.1f%%')
plt.title('Customer Activity Segments')
plt.savefig(f"{VISUALIZATIONS_DIR}/customer_activity_segments.png")
plt.close()

# 4. Customers per Region (Choropleth)
fig = px.choropleth(results['customers_per_region'],
                    locations='country',
                    locationmode='country names',
                    color='customer_count',
                    title='Customers by Country')
fig.write_image(f"{VISUALIZATIONS_DIR}/customers_per_country.png")

# 5. Active vs Less Active Customers (Bar Chart)
plt.figure(figsize=(10, 6))
sns.barplot(data=results['customer_activity_segments'], 
            x='activity_segment', 
            y='customer_count')
plt.title('Customer Activity Distribution')
plt.xlabel('Activity Segment')
plt.ylabel('Number of Customers')
plt.tight_layout()
plt.savefig(f"{VISUALIZATIONS_DIR}/customer_activity_distribution.png")
plt.close()

# Vendor Visualizations
# 1. Top Selling Vendors (Bar Graph)
plt.figure(figsize=(12, 6))
sns.barplot(data=results['top_selling_vendors'].head(10),
            x='total_revenue',
            y='vendor_name')
plt.title('Top 10 Vendors by Revenue')
plt.xlabel('Total Revenue')
plt.ylabel('Vendor')
plt.tight_layout()
plt.savefig(f"{VISUALIZATIONS_DIR}/top_vendors_by_revenue.png")
plt.close()

# 2. Vendor Monthly Profits (Line Graph)
plt.figure(figsize=(14, 8))
sns.lineplot(data=results['vendor_monthly_profits'],
             x='month',
             y='monthly_revenue',
             hue='vendor_name')
plt.title('Monthly Revenue for Top 10 Vendors')
plt.xlabel('Month')
plt.ylabel('Revenue')
plt.xticks(rotation=45)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig(f"{VISUALIZATIONS_DIR}/vendor_monthly_revenue.png")
plt.close()

# 3. Top 3 Items for Top Vendors (Horizontal Bar)
plt.figure(figsize=(14, 10))
sns.barplot(data=results['top_vendors_top_items'],
            x='total_quantity',
            y='item_name',
            hue='vendor_name',
            dodge=False)
plt.title('Top Selling Items by Vendor')
plt.xlabel('Quantity Sold')
plt.ylabel('Item')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig(f"{VISUALIZATIONS_DIR}/top_items_by_vendor.png")
plt.close()

# 4. Top Item Types for Top Vendors (Horizontal Bar)
plt.figure(figsize=(14, 10))
sns.barplot(data=results['top_vendors_item_types'],
            x='total_quantity',
            y='item_type',
            hue='vendor_name',
            dodge=False)
plt.title('Top Selling Item Types by Vendor')
plt.xlabel('Quantity Sold')
plt.ylabel('Item Type')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig(f"{VISUALIZATIONS_DIR}/top_item_types_by_vendor.png")
plt.close()

# 5. Vendor Country Distribution (Choropleth Subplots)
top_vendors = results['vendor_country_distribution']['vendor_name'].unique()
fig = make_subplots(rows=1, cols=5, subplot_titles=top_vendors)

for i, vendor in enumerate(top_vendors, 1):
    vendor_data = results['vendor_country_distribution'][results['vendor_country_distribution']['vendor_name'] == vendor]
    fig.add_trace(
        go.Choropleth(
            locations=vendor_data['country'],
            locationmode='country names',
            z=vendor_data['order_count'],
            colorscale='Blues',
            showscale=False,
            name=vendor
        ),
        row=1, col=i
    )

fig.update_layout(height=400, width=1500, title_text="Top 5 Vendors - Orders by Country")
fig.write_image(f"{VISUALIZATIONS_DIR}/vendor_country_distribution.png")

# 6. Underperforming Vendors (Table)
results['underperforming_vendors'].to_csv(f"{VISUALIZATIONS_DIR}/underperforming_vendors.csv", index=False)

#Item analytics

# plt.figure(figsize=(14, 8))
# sns.lineplot(data=results['top_items_by_orders'], 
#              x='month', 
#              y='order_count', 
#              hue='item_name',
#              style='item_name',
#              markers=True)
# plt.title('Top 10 Items by Monthly Order Count')
# plt.xlabel('Month')
# plt.ylabel('Order Count')
# plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/top_items_order_trend.png")
# plt.close()

# # 2. Top 10 Item Types by Order Counts (Line Graph)
# plt.figure(figsize=(14, 8))
# sns.lineplot(data=results['top_item_types_by_orders'], 
#              x='month', 
#              y='order_count', 
#              hue='item_type',
#              style='item_type',
#              markers=True)
# plt.title('Top 10 Item Types by Monthly Order Count')
# plt.xlabel('Month')
# plt.ylabel('Order Count')
# plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/top_item_types_order_trend.png")
# plt.close()

# # 3. Top 6 Item Types per Region (Choropleth Subplots)
# top_countries = results['top_item_types_by_region']['country'].unique()[:6]
# fig = make_subplots(rows=2, cols=3, subplot_titles=top_countries)

# for i, country in enumerate(top_countries, 1):
#     country_data = results['top_item_types_by_region'][results['top_item_types_by_region']['country'] == country]
#     fig.add_trace(
#         go.Choropleth(
#             locations=[country],
#             locationmode='country names',
#             z=[1],  # Dummy value
#             text=country_data['item_type'] + ': ' + country_data['order_count'].astype(str),
#             colorscale='Blues',
#             showscale=False
#         ),
#         row=(i-1)//3 + 1, col=(i-1)%3 + 1
#     )

# fig.update_layout(height=600, width=1200, title_text="Top Item Types by Country")
# fig.write_image(f"{VISUALIZATIONS_DIR}/item_types_by_region.png")

# # 4. Top 10 Items per Region (Choropleth Subplots)
# fig = make_subplots(rows=2, cols=5, subplot_titles=[f"Top {i+1}" for i in range(10)])

# for i in range(10):
#     item_data = results['top_items_by_region'].nlargest(10, 'order_count').iloc[i]
#     fig.add_trace(
#         go.Choropleth(
#             locations=[item_data['country']],
#             locationmode='country names',
#             z=[item_data['order_count']],
#             text=f"{item_data['item_name']}<br>Orders: {item_data['order_count']}",
#             colorscale='Blues',
#             showscale=False
#         ),
#         row=i//5 + 1, col=i%5 + 1
#     )

# fig.update_layout(height=500, width=1500, title_text="Top 10 Items by Country Distribution")
# fig.write_image(f"{VISUALIZATIONS_DIR}/top_items_by_region.png")

# # 5. Top 10 Item Types by Average Rating (Bar Graph)
# plt.figure(figsize=(12, 6))
# sns.barplot(data=results['top_item_types_ratings'],
#             x='avg_rating',
#             y='item_type')
# plt.title('Top 10 Item Types by Average Rating')
# plt.xlabel('Average Rating')
# plt.ylabel('Item Type')
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/top_item_types_ratings.png")
# plt.close()

# # 6. Underperforming Items (Table)
# results['underperforming_items'].to_csv(f"{VISUALIZATIONS_DIR}/underperforming_items.csv", index=False)

# # 7. Top 3 Item Types Monthly Orders (Bar Graph)
# plt.figure(figsize=(12, 6))
# sns.barplot(data=results['item_types_monthly_orders'],
#             x='month',
#             y='order_count',
#             hue='item_type')
# plt.title('Monthly Order Count by Top 3 Item Types')
# plt.xlabel('Month')
# plt.ylabel('Order Count')
# plt.xticks(rotation=45)
# plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/item_types_monthly_orders.png")
# plt.close()

# # Order Visualizations
# # 1. Monthly Order Counts (Bar Graph)
# plt.figure(figsize=(12, 6))
# sns.barplot(data=results['monthly_order_counts'],
#             x='month',
#             y='order_count')
# plt.title('Monthly Order Counts')
# plt.xlabel('Month')
# plt.ylabel('Number of Orders')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/monthly_order_counts.png")
# plt.close()

# # 2. Monthly Sales Estimate (Bar Graph)
# plt.figure(figsize=(12, 6))
# sns.barplot(data=results['monthly_sales_estimate'],
#             x='month',
#             y='total_sales')
# plt.title('Monthly Sales Revenue')
# plt.xlabel('Month')
# plt.ylabel('Total Sales ($)')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/monthly_sales_estimate.png")
# plt.close()

# # 3. Orders by Status (Bar Graph)
# plt.figure(figsize=(10, 6))
# sns.barplot(data=results['orders_by_status'],
#             x='status_name',
#             y='order_count')
# plt.title('Orders by Status')
# plt.xlabel('Order Status')
# plt.ylabel('Number of Orders')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig(f"{VISUALIZATIONS_DIR}/orders_by_status.png")
# plt.close()

# # 4. Orders per Region (Choropleth)
# fig = px.choropleth(results['orders_per_region'],
#                     locations='country',
#                     locationmode='country names',
#                     color='order_count',
#                     title='Orders by Country')
# fig.write_image(f"{VISUALIZATIONS_DIR}/orders_per_region.png")

# # 5. Profits per Region (Choropleth)
# fig = px.choropleth(results['profits_per_region'],
#                     locations='country',
#                     locationmode='country names',
#                     color='total_profit',
#                     title='Profits by Country')
# fig.write_image(f"{VISUALIZATIONS_DIR}/profits_per_region.png")

# print("✅ All analytics and visualizations completed!")