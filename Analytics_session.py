from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
import os

# === CONFIGURATION ===
DB_PATH = "OMS.db"
JDBC_JAR = "sqlite-jdbc-3.49.1.0.jar"
CHECKPOINT_DIR = "checkpoints"
OUTPUT_DIR = "outputs"
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

# === LOAD ALL DATA IN A SINGLE QUERY TO MINIMIZE DATABASE HITS ===
# Fetching relevant data from Orders, Order_Lines, Customers, Vendors, and Items
main_query = """
    WITH FilteredOrders AS (
        SELECT O.ID, O.Customer_ID
        FROM Orders O
        WHERE O.IS_PROCESSED = 0
    ),
    FilteredOrderLines AS (
        SELECT OL.Order_ID, OL.Item_ID, OL.Quantity
        FROM Order_Lines OL
        JOIN FilteredOrders FO ON OL.Order_ID = FO.ID
    )
    SELECT O.ID AS Order_ID, O.Customer_ID, C.Country_ID, C.City_ID, CO.Name as Country, CI.Name as City,
           OL.Item_ID, OL.Quantity, I.Vendor_ID, V.Name AS Vendor_Name, I.Name as Item_Name, IT.name AS Item_Type
    FROM Orders O
    JOIN Customers C ON O.Customer_ID = C.ID
    JOIN cITIES CI ON C.City_ID = CI.ID
    JOIN Countries CO ON C.Country_ID = CO.ID
    JOIN FilteredOrderLines OL ON O.ID = OL.Order_ID
    JOIN Items I ON OL.Item_ID = I.ID
    JOIN Item_types IT ON I.Item_type_ID = IT.ID
    JOIN Vendors V ON I.Vendor_ID = V.ID
    LIMIT 1000
"""
df = spark.sql(main_query)
df.createOrReplaceTempView("OrderData")

# === MARK ORDERS AS PROCESSED ===
# Update IS_PROCESSED flag for orders and their corresponding order lines after processing
# update_query_orders = """
#     UPDATE Orders SET IS_PROCESSED = 1 WHERE IS_PROCESSED = 0
# """
# update_query_orderlines = """
#     UPDATE Order_Lines SET IS_PROCESSED = 1 WHERE Order_ID IN (SELECT ID FROM Orders WHERE IS_PROCESSED = 1)
# """
# spark.sql(update_query_orders)
# spark.sql(update_query_orderlines)

# === ANALYTICS USING SQL ===
queries = {
    "orders_by_country": """
        SELECT Country, COUNT(DISTINCT Order_ID) AS Total_Orders
        FROM OrderData
        GROUP BY Country
    """,

    "orders_by_city": """
        SELECT City, Country, COUNT(DISTINCT Order_ID) AS Total_Orders
        FROM OrderData
        GROUP BY City, Country
    """,

    "top_customers": """
        SELECT Customer_ID, COUNT(DISTINCT Order_ID) AS Total_Orders
        FROM OrderData
        GROUP BY Customer_ID
        ORDER BY Total_Orders DESC
        LIMIT 100
    """,

    "top_selling_vendors": """
        SELECT Vendor_Name, COUNT(DISTINCT Item_ID) AS Total_Items_Sold, SUM(Quantity) AS Total_Quantity
        FROM OrderData
        GROUP BY Vendor_Name
        ORDER BY Total_Quantity DESC
    """,

    "top_items_per_vendor": """
        SELECT Vendor_Name, Item_ID, COUNT(*) AS Sold_Count
        FROM OrderData
        GROUP BY Vendor_Name, Item_ID
        ORDER BY Sold_Count DESC
    """,

    "most_sold_items": """
        SELECT Item_ID, COUNT(*) AS Count
        FROM OrderData
        GROUP BY Item_ID
        ORDER BY Count DESC
    """,

    "top_vendors_in_top_countries": """
        WITH TopCountries AS (
            SELECT Country, COUNT(DISTINCT Order_ID) AS OrderCount
            FROM OrderData
            GROUP BY Country
            ORDER BY OrderCount DESC
            LIMIT 10
        )
        SELECT Vendor_Name, TC.Country, COUNT(Item_ID) AS Items_Sold
        FROM OrderData
        JOIN TopCountries TC ON OrderData.Country = TC.Country
        GROUP BY Vendor_Name, TC.Country
        ORDER BY Items_Sold DESC
        LIMIT 10
    """
}

import os

results = {}

for key, query in queries.items():
    parquet_dir = os.path.join(OUTPUT_DIR, f"{key}.parquet")
    
    # Check if Parquet directory exists and has at least one part file
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



# results = {}
# for key, query in queries.items():
#     df = spark.sql(query)
#     results[key] = df.toPandas()
#     df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/{key}.parquet")
#     print(f"✅ Processed and saved data for {key}")

# === VISUALIZATIONS ===

# World map: orders_by_country
fig1 = px.choropleth(
    results["orders_by_country"],
    locations="Country",
    locationmode="country names",
    color="Total_Orders",
    color_continuous_scale="Plasma",
    title="Total Orders by Country"
)
fig1.write_image(f"{OUTPUT_DIR}/orders_by_country_map.png")

# Assuming 'results' contains all the Spark SQL query outputs converted to Pandas DataFrames
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")

# Create outputs directory if not exists
import os
os.makedirs("outputs", exist_ok=True)

# 1. Orders by Country
country_df = results['orders_by_country'].sort_values(by='Total_Orders', ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="Total_Orders", y="Country", data=country_df, palette="Blues_d")
plt.title("Top 10 Countries by Order Count")
plt.xlabel("Total Orders")
plt.ylabel("Country")
plt.tight_layout()
plt.savefig("outputs/orders_by_country.png")

# 2. Orders by City
city_df = results['orders_by_city'].sort_values(by='Total_Orders', ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="Total_Orders", y="City", data=city_df, palette="Purples_d")
plt.title("Top 10 Cities by Order Count")
plt.xlabel("Total Orders")
plt.ylabel("City")
plt.tight_layout()
plt.savefig("outputs/orders_by_city.png")

# 3. Top Customers
cust_df = results['top_customers'].head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="Total_Orders", y="Customer_ID", data=cust_df, palette="Greens_d")
plt.title("Top 10 Customers by Order Count")
plt.xlabel("Total Orders")
plt.ylabel("Customer ID")
plt.tight_layout()
plt.savefig("outputs/top_customers.png")

# 4. Top Selling Vendors
vendor_df = results['top_selling_vendors'].sort_values(by='Total_Quantity', ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="Total_Quantity", y="Vendor_Name", data=vendor_df, palette="Oranges_d")
plt.title("Top Vendors by Quantity Sold")
plt.xlabel("Total Quantity")
plt.ylabel("Vendor Name")
plt.tight_layout()
plt.savefig("outputs/top_selling_vendors.png")

# 5. Top Items per Vendor
items_df = results['top_items_per_vendor'].sort_values(by='Sold_Count', ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="Sold_Count", y="Item_ID", data=items_df, palette="Reds_d")
plt.title("Top Items Sold Across Vendors")
plt.xlabel("Sold Count")
plt.ylabel("Item ID")
plt.tight_layout()
plt.savefig("outputs/top_items_per_vendor.png")

# 6. Most Sold Items
most_items_df = results['most_sold_items'].head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="Count", y="Item_ID", data=most_items_df, palette="BuGn_d")
plt.title("Top 10 Most Sold Items")
plt.xlabel("Sales Count")
plt.ylabel("Item ID")
plt.tight_layout()
plt.savefig("outputs/most_sold_items.png")

# 7. Top Vendors in Top Countries
v_tc_df = results['top_vendors_in_top_countries']
plt.figure(figsize=(12, 6))
sns.barplot(data=v_tc_df, x="Items_Sold", y="Vendor_Name", hue="Country", palette="Set2")
plt.title("Top Vendors in Top Countries")
plt.xlabel("Items Sold")
plt.ylabel("Vendor Name")
plt.tight_layout()
plt.savefig("outputs/top_vendors_in_top_countries.png")


# # Bar chart: orders_by_city
# fig2 = px.bar(results["orders_by_city"], x="City", y="Total_Orders", title="Orders by City")
# fig2.write_image(f"{OUTPUT_DIR}/orders_by_city_bar.png")

# # Top customers
# fig3 = px.bar(results["top_customers"], x="Customer_ID", y="Total_Orders", title="Top 10 Customers")
# fig3.write_image(f"{OUTPUT_DIR}/top_customers.png")

# # Top vendors
# fig4 = px.bar(results["top_selling_vendors"], x="Vendor_Name", y="Total_Items_Sold", title="Top Selling Vendors")
# fig4.write_image(f"{OUTPUT_DIR}/top_vendors.png")

# # === EXPORT AS JSON-LIKE FORMAT FOR FRONTEND ===
# for key, df in results.items():
#     df.to_json(f"{OUTPUT_DIR}/{key}.json", orient="records", lines=True)
#     df.to_csv(f"{OUTPUT_DIR}/{key}.csv", index=False)

print("✅ All metrics, visualizations, and exports completed.")

update_query_orders = """
    UPDATE Orders SET IS_PROCESSED = 1 WHERE IS_PROCESSED = 0
"""
update_query_orderlines = """
    UPDATE Order_Lines SET IS_PROCESSED = 1 WHERE Order_ID IN (SELECT ID FROM Orders WHERE IS_PROCESSED = 1)
"""

jdbc_url = "jdbc:sqlite:OMS.db"

spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "(%s)" % update_query_orders) \
    .option("driver", "org.sqlite.JDBC") \
    .load()

spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "(%s)" % update_query_orderlines) \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# update_query_orders = """
#     UPDATE Orders SET IS_PROCESSED = 1 WHERE IS_PROCESSED = 0
# """
# update_query_orderlines = """
#     UPDATE Order_Lines SET IS_PROCESSED = 1 WHERE Order_ID IN (SELECT ID FROM Orders WHERE IS_PROCESSED = 1)
# """
# spark.sql(update_query_orders)
# spark.sql(update_query_orderlines)


# from pyspark.sql import SparkSession
# import pandas as pd
# import plotly.express as px
# import os

# # === CONFIGURATION ===
# DB_PATH = "OMS.db"
# JDBC_JAR = "sqlite-jdbc-3.49.1.0.jar"
# CHECKPOINT_DIR = "checkpoints"
# OUTPUT_DIR = "outputs"
# os.makedirs(OUTPUT_DIR, exist_ok=True)

# # === CREATE SPARK SESSION ===
# spark = SparkSession.builder \
#     .appName("OrderAnalytics") \
#     .config("spark.jars", JDBC_JAR) \
#     .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
#     .getOrCreate()

# # === LOAD TABLES FROM SQLITE ===
# tables = ['Orders', 'Order_lines', 'Customers', 'Vendors', 'Items', 'cities', 'countries']
# for table in tables:
#     df = spark.read.format("jdbc") \
#         .option("url", f"jdbc:sqlite:{DB_PATH}") \
#         .option("dbtable", table) \
#         .option("driver", "org.sqlite.JDBC") \
#         .load()
#     df.createOrReplaceTempView(table)

# # === READ ALREADY PROCESSED DATA ===
# def load_processed_data(key):
#     try:
#         return spark.read.parquet(f"{OUTPUT_DIR}/{key}.parquet")
#     except Exception as e:
#         print(f"❌ Failed to load processed data for {key}: {e}")
#         return None

# # === ANALYTICS USING SQL ===
# queries = {
#     "orders_by_country": """
#         WITH FilteredOrders AS (
#             SELECT ID, Customer_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Countries.name, COUNT(DISTINCT FilteredOrders.ID) AS Total_Orders
#         FROM FilteredOrders
#         JOIN Customers ON FilteredOrders.Customer_ID = Customers.id
#         JOIN Countries ON Customers.Country_ID = Countries.id
#         GROUP BY Countries.name
#     """,

#     "orders_by_city": """
#         WITH FilteredOrders AS (
#             SELECT ID, Customer_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Cities.name, Countries.name, COUNT(DISTINCT FilteredOrders.ID) AS Total_Orders
#         FROM FilteredOrders
#         JOIN Customers ON FilteredOrders.Customer_ID = Customers.id
#         JOIN Countries ON Customers.Country_ID = Countries.id
#         JOIN Cities ON Customers.City_ID = Cities.id
#         GROUP BY Cities.name
#     """,

#     "top_customers": """
#         WITH FilteredOrders AS (
#             SELECT ID, Customer_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Customers.Customer_ID, Customers.First_name, COUNT(DISTINCT FilteredOrders.ID) AS Total_Orders
#         FROM FilteredOrders
#         JOIN Customers ON FilteredOrders.Customer_ID = Customers.ID
#         GROUP BY Customers.Customer_ID, Customers.First_Name
#         ORDER BY Total_Orders DESC
#         LIMIT 100
#     """,

#     "top_selling_vendors": """
#         WITH FilteredOrders AS (
#             SELECT ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Vendors.Vendor_ID, Vendors.Name,
#                COUNT(Order_Items.Item_ID) AS Total_Items_Sold,
#                COUNT(DISTINCT Order_Items.Item_ID) AS Unique_Items_Sold
#         FROM Order_lines
#         JOIN Items ON Order_Lines.Item_ID = Items.id
#         JOIN Vendors ON Items.Vendor_ID = Vendors.id
#         JOIN FilteredOrders ON Order_Lines.Order_ID = FilteredOrders.ID
#         GROUP BY Vendors.ID, Vendors.Name
#         ORDER BY Total_Items_Sold DESC
#     """,

#     "top_items_per_vendor": """
#         WITH FilteredOrders AS (
#             SELECT ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Vendors.ID, Vendors.Name, Items.ID, COUNT(*) AS Sold_Count
#         FROM Order_Lines
#         JOIN Items ON Order_Items.Item_ID = Items.Item_ID
#         JOIN Vendors ON Items.Vendor_ID = Vendors.Vendor_ID
#         JOIN FilteredOrders ON Order_Items.Order_ID = FilteredOrders.Order_ID
#         GROUP BY Vendors.Vendor_ID, Vendors.Name, Items.Item_ID
#         ORDER BY Vendors.Vendor_ID, Sold_Count DESC
#     """,

#     "top_itemtypes_per_vendor": """
#         WITH FilteredOrders AS (
#             SELECT Order_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Vendors.Vendor_ID, Vendors.Name, Item_Types.Type_Name, COUNT(*) AS Type_Count
#         FROM Order_Items
#         JOIN Items ON Order_Items.Item_ID = Items.Item_ID
#         JOIN Vendors ON Items.Vendor_ID = Vendors.Vendor_ID
#         JOIN Item_Types ON Items.Item_Type_ID = Item_Types.Item_Type_ID
#         JOIN FilteredOrders ON Order_Items.Order_ID = FilteredOrders.Order_ID
#         GROUP BY Vendors.Vendor_ID, Vendors.Name, Item_Types.Type_Name
#         ORDER BY Vendors.Vendor_ID, Type_Count DESC
#     """,

#     "most_sold_items": """
#         WITH FilteredOrders AS (
#             SELECT Order_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Items.Item_ID, COUNT(*) AS Count
#         FROM Order_Items
#         JOIN Items ON Order_Items.Item_ID = Items.Item_ID
#         JOIN FilteredOrders ON Order_Items.Order_ID = FilteredOrders.Order_ID
#         GROUP BY Items.Item_ID
#         ORDER BY Count DESC
#     """,

#     "most_sold_itemtypes": """
#         WITH FilteredOrders AS (
#             SELECT Order_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Item_Types.Type_Name, COUNT(*) AS Count
#         FROM Order_Items
#         JOIN Items ON Order_Items.Item_ID = Items.Item_ID
#         JOIN Item_Types ON Items.Item_Type_ID = Item_Types.Item_Type_ID
#         JOIN FilteredOrders ON Order_Items.Order_ID = FilteredOrders.Order_ID
#         GROUP BY Item_Types.Type_Name
#         ORDER BY Count DESC
#     """,

#     "top_vendors_in_top_countries": """
#         WITH TopCountries AS (
#             SELECT Countries.Country_Name, COUNT(DISTINCT Orders.Order_ID) AS OrderCount
#             FROM Orders
#             JOIN Customers ON Orders.Customer_ID = Customers.Customer_ID
#             JOIN Countries ON Customers.Country_ID = Countries.Country_ID
#             WHERE Orders.IS_PROCESSED = 0
#             GROUP BY Countries.Country_Name
#             ORDER BY OrderCount DESC
#             LIMIT 50
#         ),
#         FilteredOrders AS (
#             SELECT Order_ID
#             FROM Orders
#             WHERE IS_PROCESSED = 0
#         )
#         SELECT Vendors.Name AS Vendor, Countries.Country_Name, COUNT(Order_Items.Item_ID) AS Items_Sold
#         FROM Order_Items
#         JOIN Items ON Order_Items.Item_ID = Items.Item_ID
#         JOIN Vendors ON Items.Vendor_ID = Vendors.Vendor_ID
#         JOIN FilteredOrders ON Order_Items.Order_ID = FilteredOrders.Order_ID
#         JOIN Customers ON Orders.Customer_ID = Customers.Customer_ID
#         JOIN Countries ON Customers.Country_ID = Countries.Country_ID
#         WHERE Countries.Country_Name IN (SELECT Country_Name FROM TopCountries)
#         GROUP BY Vendors.Name, Countries.Country_Name
#         ORDER BY Items_Sold DESC
#         LIMIT 10
#     """
# }


# results = {}

# for key, query in queries.items():
#     # Load existing processed data if available
#     processed_df = load_processed_data(key)
#     if processed_df is not None:
#         results[key] = processed_df
#         print(f"✅ Using cached data for {key}")
#     else:
#         # Otherwise, execute the query and save the results
#         df = spark.sql(query)
#         df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/{key}.parquet")
#         results[key] = df.toPandas()
#         print(f"✅ Processed and saved data for {key}")

# # === VISUALIZATIONS ===

# # World map: orders_by_country
# fig1 = px.choropleth(
#     results["orders_by_country"],
#     locations="Country",
#     locationmode="country names",
#     color="Total_Orders",
#     color_continuous_scale="Plasma",
#     title="Total Orders by Country"
# )
# fig1.write_image(f"{OUTPUT_DIR}/orders_by_country_map.png")

# # Bar chart: orders_by_city
# fig2 = px.bar(results["orders_by_city"], x="City", y="Total_Orders", title="Orders by City")
# fig2.write_image(f"{OUTPUT_DIR}/orders_by_city_bar.png")

# # Top customers
# fig3 = px.bar(results["top_customers"], x="Name", y="Total_Orders", title="Top 10 Customers")
# fig3.write_image(f"{OUTPUT_DIR}/top_customers.png")

# # Top vendors
# fig4 = px.bar(results["top_selling_vendors"], x="Name", y="Total_Items_Sold", title="Top Selling Vendors")
# fig4.write_image(f"{OUTPUT_DIR}/top_vendors.png")

# # === EXPORT AS JSON-LIKE FORMAT FOR FRONTEND ===
# for key, df in results.items():
#     df.to_json(f"{OUTPUT_DIR}/{key}.json", orient="records", lines=True)
#     df.to_csv(f"{OUTPUT_DIR}/{key}.csv", index=False)

# print("✅ All metrics, visualizations, and exports completed.")
