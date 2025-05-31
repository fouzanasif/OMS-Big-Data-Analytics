import sqlite3
import numpy as np
import pandas as pd
import hashlib
import random
from typing import List, Dict, Tuple, Set
import time
from dataclasses import dataclass
from pyspark.sql import SparkSession
import plotly.express as px
import os
import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors, VectorUDT
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict, Counter
import itertools

# === CONFIGURATION ===
DB_PATH = "OMS1.db"
JDBC_JAR = "sqlite-jdbc-3.49.1.0.jar"
CHECKPOINT_DIR = "checkpoints"
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


spark = SparkSession.builder \
    .appName("FrequentItemsets") \
    .master("local[6]") \
    .config("spark.executor.cores", "6") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.jars", JDBC_JAR) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

MIN_SUPPORT = 0.001
MIN_CONFIDENCE = 0.001

print("🚀 Starting PCY Frequent Itemset Mining Analysis")
print("=" * 60)

# Step 1: Data Loading and Initial Exploration
print("\n📊 Step 1: Loading and exploring data...")

items_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", 'items') \
        .option("driver", "org.sqlite.JDBC") \
        .load()

orders_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", 'orders') \
        .option("driver", "org.sqlite.JDBC") \
        .load()

order_lines_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", 'order_lines') \
        .option("driver", "org.sqlite.JDBC") \
        .load()

# Alternative: Load from files if you have CSV exports
# items_df = spark.read.csv("items.csv", header=True, inferSchema=True)
# orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
# order_lines_df = spark.read.csv("order_lines.csv", header=True, inferSchema=True)

print(f"Original data sizes:")
print(f"Items: {items_df.count():,} records")
print(f"Orders: {orders_df.count():,} records")
print(f"Order Lines: {order_lines_df.count():,} records")

# Step 2: Data Cleaning and Filtering
print("\n🧹 Step 2: Applying data filters...")

# Filter items: rating > 3 and times_purchased > 100
filtered_items = items_df.filter(
    # (col("rating") > 1) & 
    (col("times_purchased") > 20) &
    (col("active") == 1)
).select("id", "name", "times_purchased")

print(f"Filtered items (purchased>20): {filtered_items.count():,}")

# Calculate average payment for order filtering
avg_payment = orders_df.select(avg("total_payment")).collect()[0][0]
print(f"Average order payment: ${avg_payment:.2f}")

# Filter orders: total_payment > average_payment
filtered_orders = orders_df.filter(
    col("total_payment") > avg_payment
).select("id", "customer_id", "total_payment", "date_of_order")

print(f"Filtered orders (payment > average): {filtered_orders.count():,}")

# Filter order_lines: only from filtered orders and filtered items
filtered_order_lines = order_lines_df \
    .join(filtered_orders.select("id").withColumnRenamed("id", "order_id"), "order_id") \
    .join(filtered_items.select("id").withColumnRenamed("id", "item_id"), "item_id") \
    .select("order_id", "item_id", "quantity", "price_per_unit")

print(f"Filtered order lines: {filtered_order_lines.count():,}")

# Create market basket data (transactions)
print("\n🛒 Step 3: Creating market basket transactions...")

# Group items by order to create transactions
transactions = filtered_order_lines \
    .join(filtered_items.select(col("id").alias("item_id"), "name"), "item_id") \
    .groupBy("order_id") \
    .agg(collect_list("name").alias("items")) \
    .filter(size(col("items")) >= 2)  # Only consider orders with 2+ items

total_transactions = transactions.count()
print(f"Total transactions (orders with 2+ items): {total_transactions:,}")

# Calculate absolute minimum support
abs_min_support = int(total_transactions * MIN_SUPPORT)
print(f"Absolute minimum support: {abs_min_support}")

# Step 4: PCY Algorithm Implementation
print("\n🔍 Step 4: Implementing PCY Algorithm...")

# PCY Pass 1: Count individual items and hash pairs
print("PCY Pass 1: Counting items and hashing pairs...")

def hash_pair(item1, item2, num_buckets=1000000):
    """Hash function for pair counting"""
    pair_str = f"{__builtins__.min(item1, item2)}#{__builtins__.max(item1, item2)}"
    return int(hashlib.md5(pair_str.encode()).hexdigest(), 16) % num_buckets

# Collect transactions to driver for PCY processing (for large datasets, consider sampling)
print("Collecting sample transactions for PCY...")
sample_transactions = transactions.sample(False, np.min([1.0, 100000/total_transactions])).collect()
sample_size = len(sample_transactions)
print(f"Processing sample of {sample_size:,} transactions")

# Count individual items
item_counts = Counter()
bucket_counts = Counter()

for row in sample_transactions:
    items = row['items']
    # Count individual items
    for item in items:
        item_counts[item] += 1
    
    # Hash all pairs and count buckets
    for i in range(len(items)):
        for j in range(i+1, len(items)):
            bucket = hash_pair(items[i], items[j])
            bucket_counts[bucket] += 1

# Find frequent 1-itemsets
frequent_1_items = {item for item, count in item_counts.items() 
                   if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent 1-itemsets found: {len(frequent_1_items)}")

# Find frequent buckets
frequent_buckets = {bucket for bucket, count in bucket_counts.items() 
                   if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent buckets: {len(frequent_buckets):,}")

# PCY Pass 2: Count candidate pairs
print("\nPCY Pass 2: Counting candidate pairs...")

pair_counts = Counter()

for row in sample_transactions:
    items = [item for item in row['items'] if item in frequent_1_items]
    
    for i in range(len(items)):
        for j in range(i+1, len(items)):
            pair = tuple(sorted([items[i], items[j]]))
            bucket = hash_pair(items[i], items[j])
            
            # Only count if bucket is frequent
            if bucket in frequent_buckets:
                pair_counts[pair] += 1

# Find frequent 2-itemsets
frequent_2_itemsets = {pair for pair, count in pair_counts.items() 
                      if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent 2-itemsets found: {len(frequent_2_itemsets)}")

# PCY Pass 3: Generate 3-itemsets
print("\nPCY Pass 3: Generating 3-itemsets...")

# Generate candidate 3-itemsets from frequent 2-itemsets
candidate_3_itemsets = set()
frequent_2_list = list(frequent_2_itemsets)

for i in range(len(frequent_2_list)):
    for j in range(i+1, len(frequent_2_list)):
        pair1 = frequent_2_list[i]
        pair2 = frequent_2_list[j]
        
        # Check if pairs share exactly one item
        common = set(pair1) & set(pair2)
        if len(common) == 1:
            candidate = tuple(sorted(set(pair1) | set(pair2)))
            if len(candidate) == 3:
                candidate_3_itemsets.add(candidate)

print(f"Candidate 3-itemsets generated: {len(candidate_3_itemsets)}")

# Count candidate 3-itemsets
triplet_counts = Counter()

for row in sample_transactions:
    items = set(item for item in row['items'] if item in frequent_1_items)
    
    for candidate in candidate_3_itemsets:
        if set(candidate).issubset(items):
            triplet_counts[candidate] += 1

# Find frequent 3-itemsets
frequent_3_itemsets = {triplet for triplet, count in triplet_counts.items() 
                      if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent 3-itemsets found: {len(frequent_3_itemsets)}")



# Step 5: Results Analysis and Visualization
print("\n📈 Step 5: Analyzing results and creating visualizations...")

# Prepare data for visualization
def prepare_itemset_data(itemsets, counts_dict, itemset_size):
    data = []
    for itemset in itemsets:
        count = counts_dict.get(itemset, 0)
        support = count / sample_size
        if itemset_size == 2:
            data.append({
                'itemset': f"{itemset[0]} + {itemset[1]}",
                'items': list(itemset),
                'count': count,
                'support': support
            })
        elif itemset_size == 3:
            data.append({
                'itemset': f"{itemset[0]} + {itemset[1]} + {itemset[2]}",
                'items': list(itemset),
                'count': count,
                'support': support
            })
    return sorted(data, key=lambda x: x['support'], reverse=True)

# Prepare visualization data
frequent_2_data = prepare_itemset_data(frequent_2_itemsets, pair_counts, 2)
frequent_3_data = prepare_itemset_data(frequent_3_itemsets, triplet_counts, 3)


print("\n📋 Step 6: Generating Association Rules...")

def generate_association_rules(frequent_itemsets, transaction_counts, min_confidence=MIN_CONFIDENCE):
    rules = []
    
    for itemset in frequent_itemsets:
        if len(itemset) < 2:
            continue
            
        # Generate all possible antecedent-consequent combinations
        for i in range(1, len(itemset)):
            for antecedent in itertools.combinations(itemset, i):
                consequent = tuple(item for item in itemset if item not in antecedent)
                
                # Calculate support and confidence
                itemset_support = transaction_counts.get(itemset, 0) / sample_size
                antecedent_support = 0
                
                # Find antecedent support
                if len(antecedent) == 1:
                    antecedent_support = item_counts.get(antecedent[0], 0) / sample_size
                else:
                    antecedent_support = transaction_counts.get(antecedent, 0) / sample_size
                
                if antecedent_support > 0:
                    confidence = itemset_support / antecedent_support
                    
                    if confidence >= min_confidence:
                        lift = confidence / (transaction_counts.get(consequent, item_counts.get(consequent[0], 0)) / sample_size) if len(consequent) == 1 else confidence
                        
                        rules.append({
                            'antecedent': ' + '.join(antecedent),
                            'consequent': ' + '.join(consequent),
                            'support': itemset_support,
                            'confidence': confidence,
                            'lift': lift if lift != confidence else 1.0
                        })
    
    return sorted(rules, key=lambda x: x['confidence'], reverse=True)

# Generate rules from 2-itemsets and 3-itemsets

all_frequent_itemsets = list(frequent_1_items) + list(frequent_2_itemsets) + list(frequent_3_itemsets)

with open('all_frequent_itemsets.txt', 'w') as f:
    for item in all_frequent_itemsets:
        f.write(f"{item}\n")

all_frequent_itemsets = list(frequent_2_itemsets) + list(frequent_3_itemsets)
all_counts = {**pair_counts, **triplet_counts}

association_rules = generate_association_rules(all_frequent_itemsets, all_counts)

with open('association_rules.txt', 'w') as f:
    for item in association_rules:
        f.write(f"{item}\n")

# Display results
print("\n" + "="*80)
print("🎯 FINAL RESULTS SUMMARY")
print("="*80)

print(f"\n📊 Dataset Statistics:")
print(f"   • Original transactions: {total_transactions:,}")
print(f"   • Sample size processed: {sample_size:,}")
print(f"   • Average order value: ${avg_payment:.2f}")
print(f"   • Minimum support threshold: {MIN_SUPPORT} ({abs_min_support} transactions)")

print(f"\n🔍 PCY Algorithm Results:")
print(f"   • Frequent 1-itemsets: {len(frequent_1_items)}")
print(f"   • Frequent 2-itemsets: {len(frequent_2_itemsets)}")
print(f"   • Frequent 3-itemsets: {len(frequent_3_itemsets)}")
print(f"   • Association rules generated: {len(association_rules)}")

print(f"\n🏆 Top 5 Frequent 2-Itemsets:")
for i, item in enumerate(frequent_2_data[:5], 1):
    print(f"   {i}. {item['itemset']} (Support: {item['support']:.3f})")

if frequent_3_data:
    print(f"\n🏆 Top 5 Frequent 3-Itemsets:")
    for i, item in enumerate(frequent_3_data[:5], 1):
        print(f"   {i}. {item['itemset']} (Support: {item['support']:.3f})")

if association_rules:
    print(f"\n📏 Top 5 Association Rules:")
    for i, rule in enumerate(association_rules[:5], 1):
        print(f"   {i}. {rule['antecedent']} → {rule['consequent']}")
        print(f"      Support: {rule['support']:.3f}, Confidence: {rule['confidence']:.3f}, Lift: {rule['lift']:.3f}")


# # Create visualizations
# plt.style.use('seaborn-v0_8')
fig = plt.figure(figsize=(20, 12))

# Plot 1: Top frequent 2-itemsets
plt.subplot(2, 3, 1)
if frequent_2_data:
    top_pairs = frequent_2_data[:15]
    itemsets = [item['itemset'] for item in top_pairs]
    supports = [item['support'] for item in top_pairs]
    
    bars = plt.barh(range(len(itemsets)), supports, color='skyblue', alpha=0.8)
    plt.yticks(range(len(itemsets)), [item.replace(' + ', ' +\n') for item in itemsets])
    plt.xlabel('Support')
    plt.title('Top 15 Frequent 2-Itemsets', fontweight='bold', pad=20)
    plt.grid(axis='x', alpha=0.3)
    
    # Add support values on bars
    for i, (bar, support) in enumerate(zip(bars, supports)):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{support:.3f}', va='center', fontsize=8)

# Plot 2: Top frequent 3-itemsets
plt.subplot(2, 3, 2)
if frequent_3_data:
    top_triplets = frequent_3_data[:10]
    itemsets = [item['itemset'] for item in top_triplets]
    supports = [item['support'] for item in top_triplets]
    
    bars = plt.barh(range(len(itemsets)), supports, color='lightcoral', alpha=0.8)
    plt.yticks(range(len(itemsets)), [item.replace(' + ', ' +\n') for item in itemsets])
    plt.xlabel('Support')
    plt.title('Top 10 Frequent 3-Itemsets', fontweight='bold', pad=20)
    plt.grid(axis='x', alpha=0.3)
    
    # Add support values on bars
    for i, (bar, support) in enumerate(zip(bars, supports)):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{support:.3f}', va='center', fontsize=8)

# Plot 3: Support distribution
plt.subplot(2, 3, 3)
all_supports = [item['support'] for item in frequent_2_data + frequent_3_data]
if all_supports:
    plt.hist(all_supports, bins=20, alpha=0.7, color='gold', edgecolor='black')
    plt.axvline(MIN_SUPPORT, color='red', linestyle='--', linewidth=2, label=f'Min Support ({MIN_SUPPORT})')
    plt.xlabel('Support')
    plt.ylabel('Frequency')
    plt.title('Distribution of Itemset Support Values', fontweight='bold')
    plt.legend()
    plt.grid(alpha=0.3)

# Plot 4: Itemset size comparison
plt.subplot(2, 3, 4)
sizes = ['2-Itemsets', '3-Itemsets']
counts = [len(frequent_2_itemsets), len(frequent_3_itemsets)]
colors = ['skyblue', 'lightcoral']

bars = plt.bar(sizes, counts, color=colors, alpha=0.8, edgecolor='black')
plt.ylabel('Number of Frequent Itemsets')
plt.title('Frequent Itemsets by Size', fontweight='bold')
plt.grid(axis='y', alpha=0.3)

# Add count labels on bars
for bar, count in zip(bars, counts):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + __builtins__.max(counts)*0.01, 
            str(count), ha='center', va='bottom', fontweight='bold')

# Plot 5: Top individual items
plt.subplot(2, 3, 5)
top_items = sorted([(item, count) for item, count in item_counts.items() 
                   if item in frequent_1_items], key=lambda x: x[1], reverse=True)[:15]

if top_items:
    items = [item[0] for item in top_items]
    counts = [item[1] for item in top_items]
    
    bars = plt.barh(range(len(items)), counts, color='lightgreen', alpha=0.8)
    plt.yticks(range(len(items)), [item[:30] + '...' if len(item) > 30 else item for item in items])
    plt.xlabel('Frequency')
    plt.title('Top 15 Individual Items', fontweight='bold')
    plt.grid(axis='x', alpha=0.3)

# Plot 6: PCY Algorithm Performance
plt.subplot(2, 3, 6)
stages = ['Total Items', 'Frequent 1-Items', 'Candidate Pairs', 'Frequent 2-Items', 'Frequent 3-Items']
values = [len(item_counts), len(frequent_1_items), len(pair_counts), 
          len(frequent_2_itemsets), len(frequent_3_itemsets)]

bars = plt.bar(range(len(stages)), values, color=['gray', 'green', 'orange', 'blue', 'purple'], alpha=0.7)
plt.xticks(range(len(stages)), stages, rotation=45, ha='right')
plt.ylabel('Count')
plt.title('PCY Algorithm Progression', fontweight='bold')
plt.yscale('log')  # Log scale due to potentially large differences

# Add count labels
for bar, value in zip(bars, values):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.1, 
            str(value), ha='center', va='bottom', fontsize=8)

plt.tight_layout()
plt.savefig("outputs/Itemset_subplots.png")
plt.show()

# Step 6: Generate Association Rules


print("\n" + "="*80)
print("✅ Analysis Complete! Check the visualizations above for detailed insights.")
print("="*80)

# Clean up
# spark.stop()