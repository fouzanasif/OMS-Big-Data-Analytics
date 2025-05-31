import sqlite3
import numpy as np
import pandas as pd
from collections import defaultdict
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
from collections import defaultdict


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
tables = ['Orders', 'Order_lines', 'Customers', 'Items', 'Order_status', 'Ratings']
for table in tables:
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", table) \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    df.createOrReplaceTempView(table)

avg_price = 1000

json_folder_path = "user_item_sets.json"

if os.path.exists(json_folder_path) and any(f.endswith(".json") for f in os.listdir(json_folder_path)):
    user_item_sets = spark.read.json(json_folder_path)
    user_item_sets.show(5)
else:
    

    query = f"""
        WITH filtered_customers AS (
            SELECT c.id as customer_id, COUNT(DISTINCT o.id) as order_count
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE c.active = 1
            GROUP BY c.id
            HAVING COUNT(DISTINCT o.id) > 2
        ),
        filtered_items AS (
            SELECT i.id as item_id, AVG(i.rating) as avg_rating
            FROM items i
            WHERE i.active = 1 AND i.times_purchased > 85
            GROUP BY i.id
        ),
        filtered_orders AS (
            SELECT 
                o.id AS order_id, 
                o.customer_id, 
                o.total_payment
            FROM orders o
            JOIN order_status os ON o.status_id = os.id
            WHERE 
                os.status_name IN ('PLACED', 'CONFIRMED', 'DELIVERED')
                AND o.total_payment > (
                    SELECT AVG(o2.total_payment)
                    FROM orders o2
                    JOIN order_status os2 ON o2.status_id = os2.id
                    WHERE 
                        os2.status_name IN ('PLACED', 'CONFIRMED', 'DELIVERED')
                )
        )
        SELECT 
            fo.customer_id,
            ol.item_id,
            i.name,
            r.rating as item_rating
        FROM filtered_orders fo
        JOIN order_lines ol ON fo.order_id = ol.order_id
        JOIN filtered_customers fc ON fo.customer_id = fc.customer_id
        JOIN filtered_items fi ON ol.item_id = fi.item_id
        JOIN items i ON ol.item_id = i.id
        JOIN Ratings r ON 
            r.customer_id = fo.customer_id 
            AND r.order_line_id = ol.id 
            AND r.item_id = ol.item_id;
    """

    '''SELECT 
            fo.customer_id,
            ol.id as orderline_id,
            ol.item_id,
            ol.quantity,
            ol.price_per_unit,
            ol.total_price,
            i.rating as item_rating,
            i.name as item_name,
            i.price as item_price,
            r.rating as user_rating
        FROM filtered_orders fo
        JOIN order_lines ol ON fo.order_id = ol.order_id
        JOIN filtered_customers fc ON fo.customer_id = fc.customer_id
        JOIN filtered_items fi ON ol.item_id = fi.item_id
        JOIN items i ON ol.item_id = i.id
        JOIN Ratings r ON 
            r.customer_id = fo.customer_id 
            AND r.order_line_id = ol.id 
            AND r.item_id = ol.item_id;'''
            
    df = spark.sql(query)
    df.createOrReplaceTempView("OrderData")

    df.show(10, truncate=True)

    # Step 1: Data Preprocessing and User-Item Set Creation
    print("\n=== Step 1: Creating User-Item Sets ===")

    # Create user-item sets (list of items per user)
    user_item_sets = df.groupBy("customer_id").agg(
        collect_set("item_id").alias("item_set"),
        count("item_id").alias("num_items")
    )

    print("User-Item Sets:")
    user_item_sets.show(truncate=False)

    # Get all unique items for creating the universal set
    all_items = df.select("item_id").distinct().rdd.map(lambda row: row[0]).collect()
    all_items.sort()
    num_items = len(all_items)
    item_to_index = {item: idx for idx, item in enumerate(all_items)}

    print(f"Total unique items: {num_items}")
    print(f"All items: {all_items}")

# Step 2: Custom MinHash Implementation
print("\n=== Step 2: Custom MinHash Implementation ===")

class CustomMinHash:
    def __init__(self, num_hashes=100, seed=42):
        """
        Custom MinHash implementation
        
        Args:
            num_hashes: Number of hash functions to use
            seed: Random seed for reproducibility
        """
        self.num_hashes = num_hashes
        self.seed = seed
        np.random.seed(seed)
        
        # Generate random parameters for hash functions
        # Using universal hashing: h(x) = ((a*x + b) mod p) mod m
        self.a_values = np.random.randint(1, 10000, num_hashes)
        self.b_values = np.random.randint(1, 10000, num_hashes)
        self.prime = 7919
        # self.a_values = np.random.randint(1, 2**15-1, num_hashes)
        # self.b_values = np.random.randint(0, 2**15-1, num_hashes)
        # self.prime = 2147483647  # Large prime number
        
        print(f"Initialized MinHash with {num_hashes} hash functions")
    
    def hash_function(self, x, a, b):
        return (a * x + b) % self.prime
    
    def compute_minhash_signature(self, item_set, universe_size):
        """
        Compute MinHash signature for a set of items
        
        Args:
            item_set: Set of item indices
            universe_size: Size of the universal set
        
        Returns:
            MinHash signature as a list
        """
        signature = []
        
        for i in range(self.num_hashes):
            min_hash = float('inf')
            
            # Compute hash for each item in the set
            for item in item_set:
                hash_val = self.hash_function(item, self.a_values[i], self.b_values[i])
                min_hash = __builtins__.min(min_hash, hash_val)
            
            signature.append(min_hash)
        
        return signature
    
    def jaccard_similarity_estimate(self, sig1, sig2):
        """
        Estimate Jaccard similarity from MinHash signatures
        
        Args:
            sig1, sig2: MinHash signatures
        
        Returns:
            Estimated Jaccard similarity
        """
        if len(sig1) != len(sig2):
            raise ValueError("Signatures must have the same length")
        
        matches = np.sum([1 for a, b in zip(sig1, sig2) if a == b])
        return matches / len(sig1)

# Initialize custom MinHash
custom_minhash = CustomMinHash(num_hashes=100, seed=42)

# Compute MinHash signatures for all users
def compute_user_signatures(user_item_sets_df, minhash_obj, item_to_index, num_items):
    """Compute MinHash signatures for all users"""
    
    user_signatures = {}
    users_df = user_item_sets_df.collect()
    
    for row in users_df:
        user_id = row.customer_id
        item_set = row.item_set
        
        # Convert item IDs to indices
        item_indices = [item_to_index[item] for item in item_set if item in item_to_index]
        
        # Compute MinHash signature
        signature = minhash_obj.compute_minhash_signature(item_indices, num_items)
        user_signatures[user_id] = signature
        
        # print(f"User {user_id}: {len(item_set)} items -> Signature length {len(signature)}")
    
    return user_signatures

from pathlib import Path
import pickle

# Path to the pickle file
pkl_path = Path("user_signatures.pkl")

# Load the pickle file if it exists
if pkl_path.exists():
    with open(pkl_path, 'rb') as f:
        user_signatures = pickle.load(f)

    # Ensure the object is a dictionary
    if isinstance(user_signatures, dict):
        print(f"✅ Loaded user signatures from '{pkl_path.name}' with {len(user_signatures)} users.")
    else:
        raise TypeError("❌ Loaded object is not a dictionary. Check the pickle file format.")
else:    
    user_signatures = compute_user_signatures(user_item_sets, custom_minhash, item_to_index, num_items)

# print("Writing to user-item sets")
# user_item_sets.write.mode("overwrite").json("user_item_sets.json")


# import pickle

# try:
#     with open("user_signatures.pkl", "wb") as f:
#         pickle.dump(user_signatures, f)
#     print("Signatures file written")
# except Exception as e:
#     pass



# Step 3: Custom LSH Implementation
print("\n=== Step 3: Custom LSH Implementation ===")

class CustomLSH:
    def __init__(self, num_bands=20, rows_per_band=5, seed=42):
        """
        Custom LSH implementation for MinHash signatures
        
        Args:
            num_bands: Number of bands for LSH
            rows_per_band: Number of rows (hash functions) per band
            seed: Random seed
        """
        self.num_bands = num_bands
        self.rows_per_band = rows_per_band
        self.signature_length = num_bands * rows_per_band
        self.seed = seed
        
        # Hash tables - one for each band
        self.hash_tables = [defaultdict(list) for _ in range(num_bands)]
        
        print(f"Initialized LSH with {num_bands} bands, {rows_per_band} rows per band")
        print(f"Expected signature length: {self.signature_length}")
    
    def hash_band(self, band_values, band_idx):
        """Hash a band to a bucket"""
        # Convert band values to string and hash
        band_str = ','.join(map(str, band_values))
        hash_obj = hashlib.md5(f"{band_idx}_{band_str}".encode())
        return hash_obj.hexdigest()
    
    def add_signature(self, user_id, signature):
        """Add a user's MinHash signature to LSH hash tables"""
        if len(signature) < self.signature_length:
            # Pad signature if it's shorter than expected
            signature = signature + [0] * (self.signature_length - len(signature))
        
        # Split signature into bands and hash each band
        for band_idx in range(self.num_bands):
            start_idx = band_idx * self.rows_per_band
            end_idx = start_idx + self.rows_per_band
            band = signature[start_idx:end_idx]
            
            bucket = self.hash_band(band, band_idx)
            self.hash_tables[band_idx][bucket].append(user_id)
    
    def get_candidate_pairs(self):
        """Get all candidate pairs from LSH hash tables"""
        candidate_pairs = set()
        
        for band_idx, hash_table in enumerate(self.hash_tables):
            for bucket, users in hash_table.items():
                if len(users) > 1:
                    # Generate all pairs from users in the same bucket
                    for i in range(len(users)):
                        for j in range(i + 1, len(users)):
                            candidate_pairs.add((__builtins__.min(users[i], users[j]), __builtins__.max(users[i], users[j])))
        
        return candidate_pairs
    
    def find_similar_users(self, target_user_id, threshold=0.5):
        """Find users similar to a target user"""
        similar_users = []
        
        # Get all candidate pairs
        candidate_pairs = self.get_candidate_pairs()
        
        # Filter pairs involving the target user
        target_pairs = [pair for pair in candidate_pairs if target_user_id in pair]
        
        return [pair[1] if pair[0] == target_user_id else pair[0] for pair in target_pairs]

# Initialize custom LSH
custom_lsh = CustomLSH(num_bands=20, rows_per_band=5, seed=42)

# Add all user signatures to LSH
print("Adding user signatures to LSH...")
for user_id, signature in user_signatures.items():
    custom_lsh.add_signature(user_id, signature)

# Get candidate pairs
candidate_pairs = custom_lsh.get_candidate_pairs()
print(f"Found {len(candidate_pairs)} candidate similar pairs")
print("Candidate pairs:", list(candidate_pairs))


# Step 4: Similarity Computation and Verification
print("\n=== Step 4: Similarity Computation ===")

def compute_exact_jaccard_similarity(user1_items, user2_items):
    """Compute exact Jaccard similarity between two item sets"""
    set1 = set(user1_items)
    set2 = set(user2_items)
    
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    
    return intersection / union if union > 0 else 0.0

def verify_lsh_accuracy(user_signatures, candidate_pairs, user_item_sets_df, custom_minhash):
    """Verify LSH accuracy by comparing estimated vs exact similarities"""
    
    user_items_dict = {}
    for row in user_item_sets_df.collect():
        user_items_dict[row.customer_id] = row.item_set
    
    similarities = []
    
    print("Verifying candidate pairs:")
    for user1, user2 in list(candidate_pairs)[:10]:  # Check first 10 pairs
        # Exact Jaccard similarity
        exact_sim = compute_exact_jaccard_similarity(
            user_items_dict[user1], 
            user_items_dict[user2]
        )
        
        # Estimated similarity from MinHash
        estimated_sim = custom_minhash.jaccard_similarity_estimate(
            user_signatures[user1], 
            user_signatures[user2]
        )
        
        similarities.append({
            'user1': user1,
            'user2': user2,
            'exact_similarity': exact_sim,
            'estimated_similarity': estimated_sim,
            'error': __builtins__.abs(exact_sim - estimated_sim)
        })
        
        print(f"Users {user1}-{user2}: Exact={exact_sim:.3f}, Estimated={estimated_sim:.3f}, Error={__builtins__.abs(exact_sim - estimated_sim):.3f}")
    
    return similarities

similarity_results = verify_lsh_accuracy(user_signatures, candidate_pairs, user_item_sets, custom_minhash)

# Step 5: Recommendation Functions
print("\n=== Step 5: Custom Recommendation System ===")

def recommend_items_to_user(target_user_id, user_item_sets_df, custom_lsh, top_k=5):
    """Recommend items to a user based on similar users found via LSH"""
    
    # Get similar users using LSH
    similar_user_ids = custom_lsh.find_similar_users(target_user_id)
    
    if not similar_user_ids:
        print(f"No similar users found for user {target_user_id}")
        return []
    
    print(f"Found {len(similar_user_ids)} similar users for user {target_user_id}: {similar_user_ids}")
    
    # Get user's current items
    user_items_dict = {}
    for row in user_item_sets_df.collect():
        user_items_dict[row.customer_id] = set(row.item_set)
    
    target_user_items = user_items_dict.get(target_user_id, set())
    
    # Collect items from similar users
    item_recommendations = defaultdict(int)
    
    for similar_user_id in similar_user_ids:
        similar_user_items = user_items_dict.get(similar_user_id, set())
        
        # Add items that target user doesn't have
        for item in similar_user_items:
            if item not in target_user_items:
                item_recommendations[item] += 1
    
    # Sort by frequency (popularity among similar users)
    sorted_recommendations = sorted(
        item_recommendations.items(), 
        key=lambda x: x[1], 
        reverse=True
    )
    
    return sorted_recommendations[:top_k]

def recommend_users_for_item(target_item_id, user_item_sets_df, custom_lsh, top_k=5):
    """Find users who might be interested in a specific item"""
    
    # Find users who already have this item
    users_with_item = []
    user_items_dict = {}
    
    for row in user_item_sets_df.collect():
        user_items_dict[row.customer_id] = set(row.item_set)
        if target_item_id in row.item_set:
            users_with_item.append(row.customer_id)
    
    if not users_with_item:
        print(f"No users found with item {target_item_id}")
        return []
    
    print(f"Users with item {target_item_id}: {users_with_item}")
    
    # Find users similar to those who have the item
    potential_users = set()
    
    for user_id in users_with_item:
        similar_users = custom_lsh.find_similar_users(user_id)
        for similar_user in similar_users:
            if target_item_id not in user_items_dict.get(similar_user, set()):
                potential_users.add(similar_user)
    
    return list(potential_users)[:top_k]

# Example recommendations
print("\n=== Example Recommendations ===")

items_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlite:{DB_PATH}") \
    .option("dbtable", "items") \
    .option("driver", "org.sqlite.JDBC") \
    .load()
# Recommend items to user 1
print("Recommending items to user 333461:")
item_recs = recommend_items_to_user(333461, user_item_sets, custom_lsh)
for item_id, score in item_recs:
    item_name = items_df.filter(col("id") == item_id).select("name").first()
    print(f"  Item {item_id} ({item_name.name if item_name else 'Unknown'}): Score {score}")

# Recommend users for item 87144
print(f"\nRecommending users for item 87144:")
user_recs = recommend_users_for_item(87144, user_item_sets, custom_lsh)
print(f"  Potential users: {user_recs}")

# Step 6: Performance Analysis and Visualization
print("\n=== Step 6: Performance Analysis ===")

def analyze_lsh_performance():
    """Analyze LSH performance metrics"""
    
    print("LSH Performance Analysis:")
    print(f"Number of bands: {custom_lsh.num_bands}")
    print(f"Rows per band: {custom_lsh.rows_per_band}")
    print(f"Total candidate pairs: {len(candidate_pairs)}")
    
    # Calculate bucket distribution
    bucket_sizes = []
    for band_idx, hash_table in enumerate(custom_lsh.hash_tables):
        band_bucket_sizes = [len(users) for users in hash_table.values()]
        bucket_sizes.extend(band_bucket_sizes)
    
    if bucket_sizes:
        print(f"Average bucket size: {np.mean(bucket_sizes):.2f}")
        print(f"Max bucket size: {__builtins__.max(bucket_sizes)}")
        print(f"Number of non-empty buckets: {len([s for s in bucket_sizes if s > 0])}")
    
    # MinHash accuracy analysis
    if similarity_results:
        errors = [result['error'] for result in similarity_results]
        print(f"MinHash estimation error - Mean: {np.mean(errors):.4f}, Std: {np.std(errors):.4f}")

def visualize_similarity_distribution():
    """Visualize similarity distribution and errors"""
    
    if not similarity_results:
        print("No similarity results to visualize")
        return
    
    try:
        # Extract data for visualization
        exact_sims = [r['exact_similarity'] for r in similarity_results]
        estimated_sims = [r['estimated_similarity'] for r in similarity_results]
        errors = [r['error'] for r in similarity_results]
        
        # Create visualization
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Exact vs Estimated similarity scatter plot
        ax1.scatter(exact_sims, estimated_sims, alpha=0.7)
        ax1.plot([0, 1], [0, 1], 'r--', label='Perfect estimation')
        ax1.set_xlabel('Exact Jaccard Similarity')
        ax1.set_ylabel('Estimated Jaccard Similarity')
        ax1.set_title('MinHash Estimation Accuracy')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Error distribution
        ax2.hist(errors, bins=10, alpha=0.7, color='orange')
        ax2.set_xlabel('Estimation Error')
        ax2.set_ylabel('Frequency')
        ax2.set_title('MinHash Estimation Error Distribution')
        ax2.grid(True, alpha=0.3)
        
        # User similarity heatmap (for small subset)
        user_ids = list(user_signatures.keys())[:8]  # Limit to 8 users for readability
        similarity_matrix = []
        
        for user1 in user_ids:
            row = []
            for user2 in user_ids:
                if user1 == user2:
                    sim = 1.0
                else:
                    sim = custom_minhash.jaccard_similarity_estimate(
                        user_signatures[user1], 
                        user_signatures[user2]
                    )
                row.append(sim)
            similarity_matrix.append(row)
        
        sns.heatmap(similarity_matrix, annot=True, fmt='.2f', 
                   xticklabels=user_ids, yticklabels=user_ids, 
                   cmap='viridis', ax=ax3)
        ax3.set_title('User Similarity Matrix (MinHash Estimated)')
        
        # LSH bucket size distribution
        bucket_sizes = []
        for hash_table in custom_lsh.hash_tables:
            bucket_sizes.extend([len(users) for users in hash_table.values() if len(users) > 1])
        
        if bucket_sizes:
            ax4.hist(bucket_sizes, bins=__builtins__.max(10, len(set(bucket_sizes))), alpha=0.7, color='green')
            ax4.set_xlabel('Bucket Size')
            ax4.set_ylabel('Frequency')
            ax4.set_title('LSH Bucket Size Distribution')
            ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        print(f"Visualization error (expected in non-GUI environment): {e}")

# Run performance analysis
analyze_lsh_performance()
visualize_similarity_distribution()

# Step 7: Batch Processing
print("\n=== Step 7: Batch Recommendations ===")

def generate_batch_recommendations(user_item_sets_df, custom_lsh, top_k=3):
    """Generate recommendations for all users"""
    
    all_users = [row.customer_id for row in user_item_sets_df.collect()]
    batch_recommendations = {}
    
    for user_id in all_users:
        recs = recommend_items_to_user(user_id, user_item_sets_df, custom_lsh, top_k)
        batch_recommendations[user_id] = recs
        print(f"User {user_id}: {len(recs)} recommendations")
    
    return batch_recommendations

# batch_recs = generate_batch_recommendations(user_item_sets, custom_lsh)

# print("\nBatch Recommendation Summary:")
# for user_id, recommendations in batch_recs.items():
#     rec_items = [f"Item {item_id}(score:{score})" for item_id, score in recommendations]
#     print(f"User {user_id}: {rec_items}")

# Summary Statistics
print("\n=== Final Summary ===")
print(f"Dataset Statistics:")
print(f"  - Total users: {len(user_signatures)}")
# print(f"  - Total items: {num_items}")
# print(f"  - Total interactions: {df.count()}")
# print(f"  - Sparsity: {1 - (df.count() / (len(user_signatures) * num_items)):.2%}")

print(f"\nMinHash Configuration:")
print(f"  - Hash functions: {custom_minhash.num_hashes}")
print(f"  - Signature length: {custom_minhash.num_hashes}")

print(f"\nLSH Configuration:")
print(f"  - Bands: {custom_lsh.num_bands}")
print(f"  - Rows per band: {custom_lsh.rows_per_band}")
print(f"  - Candidate pairs found: {len(candidate_pairs)}")

# print(f"\nRecommendation Statistics:")
# total_recs = sum(len(recs) for recs in batch_recs.values())
# print(f"  - Total recommendations generated: {total_recs}")
# print(f"  - Average recommendations per user: {total_recs / len(batch_recs):.1f}")

# Clean up
# spark.stop()

print("\n=== Custom Collaborative Filtering with MinHash and LSH Complete ===")