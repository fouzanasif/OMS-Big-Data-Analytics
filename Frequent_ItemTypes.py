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
    .appName("ItemTypeAnalytics") \
    .master("local[6]") \
    .config("spark.executor.cores", "6") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.jars", JDBC_JAR) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

MIN_SUPPORT = 0.005
MIN_CONFIDENCE = 0.005

print("🚀 Starting PCY Item Types Frequent Itemset Mining Analysis")
print("=" * 60)

# Step 1: Data Loading and Initial Exploration
print("\n📊 Step 1: Loading and exploring data...")

items_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", 'items') \
        .option("driver", "org.sqlite.JDBC") \
        .load()

item_types_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", 'item_types') \
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

print(f"Original data sizes:")
print(f"Items: {items_df.count():,} records")
print(f"Item Types: {item_types_df.count():,} records")
print(f"Orders: {orders_df.count():,} records")
print(f"Order Lines: {order_lines_df.count():,} records")

# Display item types for reference
print("\n📋 Available Item Types:")
item_types_list = item_types_df.select("id", "name").collect()
for row in item_types_list:
    print(f"   • {row['name']} (ID: {row['id']})")

# Step 2: Data Cleaning and Filtering
print("\n🧹 Step 2: Applying data filters...")

# Filter items: rating > 1 and times_purchased > 20
filtered_items = items_df.filter(
    # (col("rating") > 1) & 
    # (col("times_purchased") > 20) &
    (col("active") == 1)
).select("id", "name", "item_type_id", "times_purchased")

print(f"Filtered items (rating>1, purchased>20): {filtered_items.count():,}")

# Calculate average payment for order filtering
avg_payment = orders_df.select(avg("total_payment")).collect()[0][0]
print(f"Average order payment: ${avg_payment:.2f}")

# Filter orders: total_payment > average_payment
filtered_orders = orders_df.filter(
    col("total_payment") > avg_payment/5.0
).select("id", "customer_id", "total_payment", "date_of_order")

print(f"Filtered orders (payment > average): {filtered_orders.count():,}")

# Filter order_lines: only from filtered orders and filtered items
filtered_order_lines = order_lines_df \
    .join(filtered_orders.select("id").withColumnRenamed("id", "order_id"), "order_id") \
    .join(filtered_items.select("id").withColumnRenamed("id", "item_id"), "item_id") \
    .select("order_id", "item_id", "quantity", "price_per_unit")

print(f"Filtered order lines: {filtered_order_lines.count():,}")

# Step 3: Create Item Type Market Basket Data
print("\n🛒 Step 3: Creating item type market basket transactions...")

# Join to get item type information and create transactions by item type
item_type_transactions = filtered_order_lines \
    .join(filtered_items.select(col("id").alias("item_id"), "item_type_id"), "item_id") \
    .join(item_types_df.select(col("id").alias("item_type_id"), col("name").alias("item_type_name")), "item_type_id") \
    .groupBy("order_id") \
    .agg(
        collect_set("item_type_name").alias("item_types"),
        sum("quantity").alias("total_quantity"),
        sum(col("quantity") * col("price_per_unit")).alias("total_value")
    ) \
    .filter(size(col("item_types")) >= 2)  # Only consider orders with 2+ different item types

total_transactions = item_type_transactions.count()
print(f"Total transactions (orders with 2+ item types): {total_transactions:,}")

# Calculate absolute minimum support
abs_min_support = int(total_transactions * MIN_SUPPORT)
print(f"Absolute minimum support: {abs_min_support}")

# Show sample transactions
print("\n📋 Sample Item Type Transactions:")
sample_display = item_type_transactions.limit(5).collect()
for i, row in enumerate(sample_display, 1):
    print(f"   Transaction {i}: {', '.join(row['item_types'])} (Qty: {row['total_quantity']}, Value: ${row['total_value']:.2f})")

# Step 4: PCY Algorithm Implementation for Item Types
print("\n🔍 Step 4: Implementing PCY Algorithm for Item Types...")

def hash_pair(item1, item2, num_buckets=1000000):
    """Hash function for pair counting"""
    # item1, item2 = str(item1), str(item2)
    pair_str = f"{__builtins__.min(item1, item2)}#{__builtins__.max(item1, item2)}"
    return int(hashlib.md5(pair_str.encode()).hexdigest(), 16) % int(num_buckets)

# PCY Pass 1: Count individual item types and hash pairs
print("PCY Pass 1: Counting item types and hashing pairs...")

# Collect transactions to driver for PCY processing
print("Collecting sample transactions for PCY...")
sample_transactions = item_type_transactions.sample(False, np.min([1.0, 100000/total_transactions])).collect()
print("Here-1")
sample_size = len(sample_transactions)
print(f"Processing sample of {sample_size:,} transactions")

# Count individual item types
item_type_counts = Counter()
bucket_counts = Counter()

for row in sample_transactions:
    item_types = row['item_types']
    # Count individual item types
    for item_type in item_types:
        item_type_counts[item_type] += 1
    
    # Hash all pairs and count buckets
    for i in range(len(item_types)):
        for j in range(i+1, len(item_types)):
            bucket = hash_pair(item_types[i], item_types[j])
            bucket_counts[bucket] += 1

# Find frequent 1-itemsets (item types)
frequent_1_item_types = {item_type for item_type, count in item_type_counts.items() 
                        if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent 1-itemsets (item types) found: {len(frequent_1_item_types)}")
print("Frequent item types:", list(frequent_1_item_types))

# Find frequent buckets
frequent_buckets = {bucket for bucket, count in bucket_counts.items() 
                   if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent buckets: {len(frequent_buckets):,}")

# PCY Pass 2: Count candidate pairs
print("\nPCY Pass 2: Counting candidate item type pairs...")

pair_counts = Counter()

for row in sample_transactions:
    item_types = [item_type for item_type in row['item_types'] if item_type in frequent_1_item_types]
    
    for i in range(len(item_types)):
        for j in range(i+1, len(item_types)):
            pair = tuple(sorted([item_types[i], item_types[j]]))
            bucket = hash_pair(item_types[i], item_types[j])
            
            # Only count if bucket is frequent
            if bucket in frequent_buckets:
                pair_counts[pair] += 1

# Find frequent 2-itemsets
frequent_2_itemsets = {pair for pair, count in pair_counts.items() 
                      if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent 2-itemsets (item type pairs) found: {len(frequent_2_itemsets)}")

# PCY Pass 3: Generate 3-itemsets
print("\nPCY Pass 3: Generating 3-itemsets...")

# Generate candidate 3-itemsets from frequent 2-itemsets
candidate_3_itemsets = set()
frequent_2_list = list(frequent_2_itemsets)

for i in range(len(frequent_2_list)):
    for j in range(i+1, len(frequent_2_list)):
        pair1 = frequent_2_list[i]
        pair2 = frequent_2_list[j]
        
        # Check if pairs share exactly one item type
        common = set(pair1) & set(pair2)
        if len(common) == 1:
            candidate = tuple(sorted(set(pair1) | set(pair2)))
            if len(candidate) == 3:
                candidate_3_itemsets.add(candidate)

print(f"Candidate 3-itemsets generated: {len(candidate_3_itemsets)}")

# Count candidate 3-itemsets
triplet_counts = Counter()

for row in sample_transactions:
    item_types = set(item_type for item_type in row['item_types'] if item_type in frequent_1_item_types)
    
    for candidate in candidate_3_itemsets:
        if set(candidate).issubset(item_types):
            triplet_counts[candidate] += 1

# Find frequent 3-itemsets
frequent_3_itemsets = {triplet for triplet, count in triplet_counts.items() 
                      if count >= abs_min_support * (sample_size / total_transactions)}

print(f"Frequent 3-itemsets found: {len(frequent_3_itemsets)}")

# Step 5: Advanced Item Type Analytics
print("\n📊 Step 5: Advanced Item Type Analytics...")

# Calculate item type co-occurrence matrix
print("Creating item type co-occurrence analysis...")

# Get all unique item types
all_item_types = list(frequent_1_item_types)
n_types = len(all_item_types)

# Create co-occurrence matrix
cooccurrence_matrix = np.zeros((n_types, n_types))
type_to_idx = {item_type: i for i, item_type in enumerate(all_item_types)}

for row in sample_transactions:
    item_types = [it for it in row['item_types'] if it in frequent_1_item_types]
    for i in range(len(item_types)):
        for j in range(len(item_types)):
            if i != j:
                idx_i = type_to_idx[item_types[i]]
                idx_j = type_to_idx[item_types[j]]
                cooccurrence_matrix[idx_i][idx_j] += 1

# Normalize by total transactions
cooccurrence_matrix = cooccurrence_matrix / sample_size

# Step 6: Results Analysis and Visualization
print("\n📈 Step 6: Analyzing results and creating visualizations...")

def prepare_itemset_data(itemsets, counts_dict, itemset_size):
    data = []
    for itemset in itemsets:
        count = counts_dict.get(itemset, 0)
        support = count / sample_size
        if itemset_size == 1:
            data.append({
                'itemset': itemset,
                'items': [itemset],
                'count': count,
                'support': support
            })
        elif itemset_size == 2:
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
frequent_1_data = prepare_itemset_data(frequent_1_item_types, item_type_counts, 1)
frequent_2_data = prepare_itemset_data(frequent_2_itemsets, pair_counts, 2)
frequent_3_data = prepare_itemset_data(frequent_3_itemsets, triplet_counts, 3)

# Step 7: Generate Association Rules for Item Types
print("\n📋 Step 7: Generating Association Rules for Item Types...")

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
                    antecedent_support = item_type_counts.get(antecedent[0], 0) / sample_size
                else:
                    antecedent_support = transaction_counts.get(antecedent, 0) / sample_size
                
                if antecedent_support > 0:
                    confidence = itemset_support / antecedent_support
                    
                    if confidence >= min_confidence:
                        # Calculate lift
                        consequent_support = 0
                        if len(consequent) == 1:
                            consequent_support = item_type_counts.get(consequent[0], 0) / sample_size
                        else:
                            consequent_support = transaction_counts.get(consequent, 0) / sample_size
                        
                        lift = confidence / consequent_support if consequent_support > 0 else 1.0
                        
                        rules.append({
                            'antecedent': ' + '.join(antecedent),
                            'consequent': ' + '.join(consequent),
                            'support': itemset_support,
                            'confidence': confidence,
                            'lift': lift,
                            'antecedent_support': antecedent_support,
                            'consequent_support': consequent_support
                        })
    
    return sorted(rules, key=lambda x: x['confidence'], reverse=True)

# Generate rules from 2-itemsets and 3-itemsets
all_frequent_itemsets = list(frequent_2_itemsets) + list(frequent_3_itemsets)
all_counts = {**pair_counts, **triplet_counts}

association_rules = generate_association_rules(all_frequent_itemsets, all_counts)

# Save results to files
with open(f'{OUTPUT_DIR}/item_type_frequent_itemsets.txt', 'w') as f:
    f.write("=== FREQUENT ITEM TYPE ITEMSETS ===\n\n")
    f.write("1-Itemsets (Individual Item Types):\n")
    for item_type in sorted(frequent_1_item_types):
        support = item_type_counts[item_type] / sample_size
        f.write(f"  {item_type}: {support:.4f}\n")
    
    f.write("\n2-Itemsets (Item Type Pairs):\n")
    for itemset in sorted(frequent_2_itemsets):
        support = pair_counts[itemset] / sample_size
        f.write(f"  {' + '.join(itemset)}: {support:.4f}\n")
    
    f.write("\n3-Itemsets (Item Type Triplets):\n")
    for itemset in sorted(frequent_3_itemsets):
        support = triplet_counts[itemset] / sample_size
        f.write(f"  {' + '.join(itemset)}: {support:.4f}\n")

with open(f'{OUTPUT_DIR}/item_type_association_rules.txt', 'w') as f:
    f.write("=== ITEM TYPE ASSOCIATION RULES ===\n\n")
    for i, rule in enumerate(association_rules, 1):
        f.write(f"{i}. {rule['antecedent']} → {rule['consequent']}\n")
        f.write(f"   Support: {rule['support']:.4f}\n")
        f.write(f"   Confidence: {rule['confidence']:.4f}\n")
        f.write(f"   Lift: {rule['lift']:.4f}\n")
        f.write(f"   Antecedent Support: {rule['antecedent_support']:.4f}\n")
        f.write(f"   Consequent Support: {rule['consequent_support']:.4f}\n\n")

# Display results
print("\n" + "="*80)
print("🎯 ITEM TYPE ANALYSIS FINAL RESULTS")
print("="*80)

print(f"\n📊 Dataset Statistics:")
print(f"   • Total item type transactions: {total_transactions:,}")
print(f"   • Sample size processed: {sample_size:,}")
print(f"   • Average order value: ${avg_payment:.2f}")
print(f"   • Minimum support threshold: {MIN_SUPPORT} ({abs_min_support} transactions)")
print(f"   • Unique item types in dataset: {len(item_type_counts)}")

print(f"\n🔍 PCY Algorithm Results:")
print(f"   • Frequent 1-itemsets (item types): {len(frequent_1_item_types)}")
print(f"   • Frequent 2-itemsets (type pairs): {len(frequent_2_itemsets)}")
print(f"   • Frequent 3-itemsets (type triplets): {len(frequent_3_itemsets)}")
print(f"   • Association rules generated: {len(association_rules)}")

print(f"\n🏆 Top 5 Individual Item Types:")
for i, item in enumerate(frequent_1_data[:5], 1):
    print(f"   {i}. {item['itemset']} (Support: {item['support']:.3f})")

print(f"\n🏆 Top 5 Item Type Pairs:")
for i, item in enumerate(frequent_2_data[:5], 1):
    print(f"   {i}. {item['itemset']} (Support: {item['support']:.3f})")

if frequent_3_data:
    print(f"\n🏆 Top 5 Item Type Triplets:")
    for i, item in enumerate(frequent_3_data[:5], 1):
        print(f"   {i}. {item['itemset']} (Support: {item['support']:.3f})")

if association_rules:
    print(f"\n📏 Top 5 Association Rules:")
    for i, rule in enumerate(association_rules[:5], 1):
        print(f"   {i}. {rule}")

# Create comprehensive visualizations
fig = plt.figure(figsize=(24, 16))

# Plot 1: Individual item type frequency
plt.subplot(3, 4, 1)
if frequent_1_data:
    top_types = frequent_1_data[:10]
    item_types = [item['itemset'] for item in top_types]
    supports = [item['support'] for item in top_types]
    
    bars = plt.barh(range(len(item_types)), supports, color='lightblue', alpha=0.8)
    plt.yticks(range(len(item_types)), item_types)
    plt.xlabel('Support')
    plt.title('Individual Item Type Frequency', fontweight='bold', pad=20)
    plt.grid(axis='x', alpha=0.3)
    
    for i, (bar, support) in enumerate(zip(bars, supports)):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{support:.3f}', va='center', fontsize=8)

# Plot 2: Top frequent 2-itemsets (item type pairs)
plt.subplot(3, 4, 2)
if frequent_2_data:
    top_pairs = frequent_2_data[:10]
    itemsets = [item['itemset'] for item in top_pairs]
    supports = [item['support'] for item in top_pairs]
    
    bars = plt.barh(range(len(itemsets)), supports, color='skyblue', alpha=0.8)
    plt.yticks(range(len(itemsets)), [item.replace(' + ', ' +\n') for item in itemsets])
    plt.xlabel('Support')
    plt.title('Top 10 Item Type Pairs', fontweight='bold', pad=20)
    plt.grid(axis='x', alpha=0.3)
    
    # Add support values on bars
    for i, (bar, support) in enumerate(zip(bars, supports)):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{support:.3f}', va='center', fontsize=8)

# Plot 3: Top frequent 3-itemsets
plt.subplot(3, 4, 3)
if frequent_3_data:
    top_triplets = frequent_3_data[:8]
    itemsets = [item['itemset'] for item in top_triplets]
    supports = [item['support'] for item in top_triplets]
    
    bars = plt.barh(range(len(itemsets)), supports, color='lightcoral', alpha=0.8)
    plt.yticks(range(len(itemsets)), [item.replace(' + ', ' +\n') for item in itemsets])
    plt.xlabel('Support')
    plt.title('Top 8 Item Type Triplets', fontweight='bold', pad=20)
    plt.grid(axis='x', alpha=0.3)
    
    # Add support values on bars
    for i, (bar, support) in enumerate(zip(bars, supports)):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{support:.3f}', va='center', fontsize=8)

# Plot 4: Support distribution
plt.subplot(3, 4, 4)
all_supports = [item['support'] for item in frequent_1_data + frequent_2_data + frequent_3_data]
if all_supports:
    plt.hist(all_supports, bins=15, alpha=0.7, color='gold', edgecolor='black')
    plt.axvline(MIN_SUPPORT, color='red', linestyle='--', linewidth=2, label=f'Min Support ({MIN_SUPPORT})')
    plt.xlabel('Support')
    plt.ylabel('Frequency')
    plt.title('Distribution of Item Type Support Values', fontweight='bold')
    plt.legend()
    plt.grid(alpha=0.3)

# Plot 5: Itemset size comparison
plt.subplot(3, 4, 5)
sizes = ['1-Itemsets', '2-Itemsets', '3-Itemsets']
counts = [len(frequent_1_item_types), len(frequent_2_itemsets), len(frequent_3_itemsets)]
colors = ['lightgreen', 'skyblue', 'lightcoral']

bars = plt.bar(sizes, counts, color=colors, alpha=0.8, edgecolor='black')
plt.ylabel('Number of Frequent Itemsets')
plt.title('Frequent Item Type Itemsets by Size', fontweight='bold')
plt.grid(axis='y', alpha=0.3)

# Add count labels on bars
for bar, count in zip(bars, counts):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + __builtins__.max(counts)*0.01, 
            str(count), ha='center', va='bottom', fontweight='bold')

# Plot 6: PCY Algorithm Performance
plt.subplot(3, 4, 6)
stages = ['Total Types', 'Frequent 1-Types', 'Candidate Pairs', 'Frequent 2-Types', 'Frequent 3-Types']
values = [len(item_type_counts), len(frequent_1_item_types), len(pair_counts), 
          len(frequent_2_itemsets), len(frequent_3_itemsets)]

bars = plt.bar(range(len(stages)), values, color=['gray', 'green', 'orange', 'blue', 'purple'], alpha=0.7)
plt.xticks(range(len(stages)), stages, rotation=45, ha='right')
plt.ylabel('Count (Log Scale)')
plt.title('PCY Algorithm Progression', fontweight='bold')
plt.yscale('log')

# Add count labels
for bar, value in zip(bars, values):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.1, 
            str(value), ha='center', va='bottom', fontsize=8)

# Plot 7: Co-occurrence Heatmap
plt.subplot(3, 4, 7)
if len(all_item_types) > 1:
    mask = cooccurrence_matrix == 0
    sns.heatmap(cooccurrence_matrix, 
                xticklabels=all_item_types, 
                yticklabels=all_item_types,
                annot=True, 
                fmt='.3f', 
                cmap='YlOrRd', 
                mask=mask,
                cbar_kws={'label': 'Co-occurrence Rate'})
    plt.title('Item Type Co-occurrence Matrix', fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)

# Plot 8: Transaction size distribution
plt.subplot(3, 4, 8)
transaction_sizes = [len(row['item_types']) for row in sample_transactions]
plt.hist(transaction_sizes, bins=range(2, __builtins__.max(transaction_sizes)+2), alpha=0.7, color='purple', edgecolor='black')
plt.xlabel('Number of Item Types per Transaction')
plt.ylabel('Frequency')
plt.title('Transaction Size Distribution', fontweight='bold')
plt.grid(axis='y', alpha=0.3)

# Plot 9-12: Detailed analysis for top item type combinations
for plot_idx, (title, data, color) in enumerate([
    ('Top Item Type Pairs - Detailed', frequent_2_data[:6], 'lightblue'),
    ('Top Item Type Triplets - Detailed', frequent_3_data[:4], 'lightgreen'),
], 9):
    
    plt.subplot(3, 4, plot_idx)
    if data:
        itemsets = [item['itemset'] for item in data]
        counts = [item['count'] for item in data]
        
        bars = plt.barh(range(len(itemsets)), counts, color=color, alpha=0.8)
        plt.yticks(range(len(itemsets)), [item.replace(' + ', '\n+ ') for item in itemsets])
        plt.xlabel('Absolute Count')
        plt.title(title, fontweight='bold')
        plt.grid(axis='x', alpha=0.3)
        
        # Add count labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            plt.text(bar.get_width() + __builtins__.max(counts)*0.01, bar.get_y() + bar.get_height()/2, 
                    str(count), va='center', fontsize=8)

# Plot 11: Average transaction value by item type diversity
plt.subplot(3, 4, 11)
value_by_diversity = {}
for row in sample_transactions:
    diversity = len(row['item_types'])
    if diversity not in value_by_diversity:
        value_by_diversity[diversity] = []
    value_by_diversity[diversity].append(row['total_value'])

diversities = sorted(value_by_diversity.keys())
avg_values = [np.mean(value_by_diversity[d]) for d in diversities]

plt.plot(diversities, avg_values, marker='o', linewidth=2, markersize=8, color='darkgreen')
plt.xlabel('Number of Item Types in Transaction')
plt.ylabel('Average Transaction Value ($)')
plt.title('Transaction Value vs Item Type Diversity', fontweight='bold')
plt.grid(alpha=0.3)

# Plot 12: Quantity analysis
plt.subplot(3, 4, 12)
quantity_by_diversity = {}
for row in sample_transactions:
    diversity = len(row['item_types'])
    if diversity not in quantity_by_diversity:
        quantity_by_diversity[diversity] = []
    quantity_by_diversity[diversity].append(row['total_quantity'])

avg_quantities = [np.mean(quantity_by_diversity[d]) for d in diversities]

plt.plot(diversities, avg_quantities, marker='s', linewidth=2, markersize=8, color='darkorange')
plt.xlabel('Number of Item Types in Transaction')
plt.ylabel('Average Total Quantity')
plt.title('Quantity vs Item Type Diversity', fontweight='bold')
plt.grid(alpha=0.3)

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/ItemType_Analysis_Complete.png", dpi=300, bbox_inches='tight')
plt.show()
              