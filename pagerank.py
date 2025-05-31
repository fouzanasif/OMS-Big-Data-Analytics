import os
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime

# Initialize Spark Session with optimized configuration
def initialize_spark():
    DB_PATH = "OMS.db"
    JDBC_JAR = "sqlite-jdbc-3.49.1.0.jar"
    CHECKPOINT_DIR = "pgr_checkpoints"
    OUTPUT_DIR = "pgr_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    """Initialize Spark session with appropriate configurations"""
    spark = SparkSession.builder \
    .appName("VendorItemTypePageRank") \
    .master("local[6]") \
    .config("spark.executor.cores", "6") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.jars", JDBC_JAR) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()
    
    # spark = SparkSession.builder \
    #     .appName("VendorItemTypePageRank") \
    #     .config("spark.sql.adaptive.enabled", "true") \
    #     .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    #     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    #     .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session initialized successfully")
    return spark

# Load and prepare data from SQLite database
def load_data(spark, db_path="OMS.db"):
    """Load data from SQLite database and create DataFrames"""
    
    # Database connection properties
    db_url = f"jdbc:sqlite:{db_path}"
    connection_properties = {
        "driver": "org.sqlite.JDBC",
        "url": db_url
    }
    
    try:
        # Load tables using Spark SQL
        vendors_df = spark.read.jdbc(url=db_url, table="vendors", properties=connection_properties)
        vendors_df.createOrReplaceTempView("vendors")
        
        items_df = spark.read.jdbc(url=db_url, table="items", properties=connection_properties)
        item_types_df = spark.read.jdbc(url=db_url, table="item_types", properties=connection_properties)
        item_types_df.createOrReplaceTempView("item_types")
        order_lines_df = spark.read.jdbc(url=db_url, table="order_lines", properties=connection_properties)
        ratings_df = spark.read.jdbc(url=db_url, table="ratings", properties=connection_properties)
        
        print("✅ Data loaded successfully from database")
        return vendors_df, items_df, item_types_df, order_lines_df, ratings_df
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return [None]*5
        # Create sample data for demonstration
        # return create_sample_data(spark)

# Data preprocessing and feature engineering
def preprocess_data(spark, vendors_df, items_df, item_types_df, order_lines_df, ratings_df):
    """Preprocess data and create comprehensive metrics"""
    
    print("🔄 Starting data preprocessing...")
    
    # Register DataFrames as temporary views for SQL operations
    vendors_df.createOrReplaceTempView("vendors")
    items_df.createOrReplaceTempView("items")
    item_types_df.createOrReplaceTempView("item_types")
    order_lines_df.createOrReplaceTempView("order_lines")
    ratings_df.createOrReplaceTempView("ratings")
    
    # Create comprehensive vendor-itemtype metrics using Spark SQL
    vendor_itemtype_metrics = spark.sql("""
    WITH base_metrics AS (
        SELECT 
            v.id as vendor_id,
            v.name as vendor_name,
            it.id as item_type_id,
            it.name as item_type_name,
            
            -- Sales metrics
            COUNT(DISTINCT ol.order_id) as total_orders,
            SUM(ol.quantity) as total_quantity_sold,
            SUM(ol.quantity * ol.price_per_unit) as total_revenue,
            AVG(ol.price_per_unit) as avg_price,
            
            -- Item metrics
            COUNT(DISTINCT i.id) as total_items,
            SUM(i.quantity_in_stock) as total_stock,
            AVG(i.rating) as avg_item_rating,
            SUM(i.times_purchased) as total_purchases,
            
            -- Rating metrics
            COUNT(r.id) as total_ratings,
            AVG(r.rating) as avg_customer_rating,
            
            -- Market presence
            COUNT(DISTINCT i.sku) as unique_skus
            
        FROM vendors v
        CROSS JOIN item_types it
        LEFT JOIN items i ON v.id = i.vendor_id AND it.id = i.item_type_id
        LEFT JOIN order_lines ol ON i.id = ol.item_id
        LEFT JOIN ratings r ON i.id = r.item_id
        WHERE v.active = 1
        GROUP BY v.id, v.name, it.id, it.name
    ),
    normalized_metrics AS (
        SELECT *,
            -- Normalize metrics to 0-1 scale for PageRank
            CASE WHEN MAX(total_orders) OVER() > 0 
                 THEN total_orders / MAX(total_orders) OVER() 
                 ELSE 0 END as norm_orders,
                 
            CASE WHEN MAX(total_revenue) OVER() > 0 
                 THEN total_revenue / MAX(total_revenue) OVER() 
                 ELSE 0 END as norm_revenue,
                 
            CASE WHEN MAX(total_quantity_sold) OVER() > 0 
                 THEN total_quantity_sold / MAX(total_quantity_sold) OVER() 
                 ELSE 0 END as norm_quantity,
                 
            CASE WHEN MAX(avg_customer_rating) OVER() > 0 
                 THEN avg_customer_rating / MAX(avg_customer_rating) OVER() 
                 ELSE 0 END as norm_rating,
                 
            CASE WHEN MAX(total_items) OVER() > 0 
                 THEN total_items / MAX(total_items) OVER() 
                 ELSE 0 END as norm_items,
                 
            CASE WHEN MAX(total_purchases) OVER() > 0 
                 THEN total_purchases / MAX(total_purchases) OVER() 
                 ELSE 0 END as norm_purchases
                 
        FROM base_metrics
    )
    SELECT *,
        -- Create composite popularity score
        (norm_orders * 0.25 + 
         norm_revenue * 0.25 + 
         norm_quantity * 0.20 + 
         norm_rating * 0.15 + 
         norm_items * 0.10 + 
         norm_purchases * 0.05) as popularity_score,
         
        -- Create node identifier for PageRank
        CONCAT(vendor_id, '_', item_type_id) as node_id
        
    FROM normalized_metrics
    ORDER BY popularity_score DESC
    """)
    
    # Cache the result for better performance
    vendor_itemtype_metrics.cache()
    print(f"✅ Preprocessed {vendor_itemtype_metrics.count()} vendor-itemtype combinations")
    
    # Create checkpoint
    checkpoint_path = "checkpoints/preprocessed_data"
    vendor_itemtype_metrics.write.mode("overwrite").parquet(checkpoint_path)
    print(f"💾 Preprocessed data saved to checkpoint: {checkpoint_path}")
    
    return vendor_itemtype_metrics

# Build transition matrix for PageRank
def build_transition_matrix(spark, metrics_df):
    """Build transition matrix based on business relationships and similarities"""
    
    print("🔧 Building transition matrix...")
    
    # Get all nodes
    nodes = metrics_df.select("node_id", "vendor_id", "item_type_id", "popularity_score").collect()
    nodes = nodes[:1000]
    # node_count = len(nodes)
    node_count = 1000
    node_to_idx = {node.node_id: idx for idx, node in enumerate(nodes)}
    
    print(f"📊 Building matrix for {node_count} nodes")
    
    # Initialize transition matrix
    transition_matrix = np.zeros((node_count, node_count))
    
    # Build relationships based on multiple criteria
    for i, node_i in enumerate(nodes):
        total_weight = 0
        
        for j, node_j in enumerate(nodes):
            if i != j:
                weight = 0
                
                # Same vendor relationship (strong connection)
                if node_i.vendor_id == node_j.vendor_id:
                    weight += 0.4
                
                # Same item type relationship (moderate connection)
                if node_i.item_type_id == node_j.item_type_id:
                    weight += 0.3
                
                # Popularity-based connection (weak connection)
                popularity_diff = __builtins__.abs(node_i.popularity_score - node_j.popularity_score)
                if popularity_diff < 0.2:  # Similar popularity levels
                    weight += 0.2
                
                # Competition factor (very weak connection)
                if (node_i.vendor_id != node_j.vendor_id and 
                    node_i.item_type_id == node_j.item_type_id):
                    weight += 0.1
                
                transition_matrix[i][j] = weight
                total_weight += weight
        
        # Normalize row to make it stochastic
        if total_weight > 0:
            transition_matrix[i] = transition_matrix[i] / total_weight
        else:
            # If no outgoing connections, connect to all nodes equally
            transition_matrix[i] = np.full(node_count, 1.0 / node_count)
    
    print("✅ Transition matrix built successfully")
    
    # Save transition matrix info
    matrix_info = {
        "node_count": node_count,
        "node_mapping": {node.node_id: idx for idx, node in enumerate(nodes)},
        "matrix_stats": {
            "min_value": float(transition_matrix.min()),
            "max_value": float(transition_matrix.max()),
            "mean_value": float(transition_matrix.mean()),
            "sparsity": float(np.count_nonzero(transition_matrix) / (node_count * node_count))
        }
    }
    
    with open("checkpoints/transition_matrix_info.json", "w") as f:
        json.dump(matrix_info, f, indent=2)
    
    return transition_matrix, nodes, node_to_idx

# Power method PageRank implementation
def pagerank_power_method(transition_matrix, damping_factor=0.85, max_iterations=100, tolerance=1e-6):
    """Implement PageRank using power method"""
    
    print(f"🚀 Starting PageRank with damping factor: {damping_factor}")
    
    n = transition_matrix.shape[0]
    
    # Initialize PageRank vector uniformly
    pagerank_vector = np.full(n, 1.0 / n)
    
    # Teleportation matrix
    teleport_prob = (1 - damping_factor) / n
    
    convergence_history = []
    
    for iteration in range(max_iterations):
        # Power method step
        new_pagerank = (damping_factor * transition_matrix.T @ pagerank_vector + 
                       np.full(n, teleport_prob))
        
        # Check convergence
        diff = np.linalg.norm(new_pagerank - pagerank_vector, 1)
        convergence_history.append(diff)
        
        if diff < tolerance:
            print(f"✅ Converged after {iteration + 1} iterations")
            break
            
        pagerank_vector = new_pagerank
        
        if (iteration + 1) % 10 == 0:
            print(f"   Iteration {iteration + 1}: L1 difference = {diff:.8f}")
    
    # Normalize PageRank scores
    pagerank_vector = pagerank_vector / np.sum(pagerank_vector)
    
    return pagerank_vector, convergence_history

# Process results and create final rankings
def process_results(spark, pagerank_scores, nodes, metrics_df):
    """Process PageRank results and create final rankings"""
    
    print("📋 Processing PageRank results...")
    
    # Create results DataFrame
    results_data = []
    for i, node in enumerate(nodes):
        results_data.append({
            "node_id": node.node_id,
            "vendor_id": node.vendor_id,
            "item_type_id": node.item_type_id,
            "pagerank_score": float(pagerank_scores[i]),
            "popularity_score": float(node.popularity_score),
            "rank": 0  # Will be filled after sorting
        })
    
    # Sort by PageRank score and assign ranks
    results_data.sort(key=lambda x: x["pagerank_score"], reverse=True)
    for i, result in enumerate(results_data):
        result["rank"] = i + 1
    
    # Convert to Spark DataFrame
    results_schema = StructType([
        StructField("node_id", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("item_type_id", IntegerType(), True),
        StructField("pagerank_score", DoubleType(), True),
        StructField("popularity_score", DoubleType(), True),
        StructField("rank", IntegerType(), True)
    ])
    
    results_df = spark.createDataFrame(results_data, results_schema)
    results_df.createOrReplaceTempView("results")
    
    # Join with vendor and item type names
    results_with_names = spark.sql("""
    SELECT 
        r.*,
        v.name as vendor_name,
        it.name as item_type_name,
        m.total_orders,
        m.total_revenue,
        m.total_quantity_sold,
        m.avg_customer_rating,
        m.total_items
    FROM results r
    JOIN vendors v ON r.vendor_id = v.id
    JOIN item_types it ON r.item_type_id = it.id
    JOIN metrics m ON r.node_id = m.node_id
    ORDER BY r.pagerank_score DESC
    """)
    
    # Register results for SQL queries
    
    results_with_names.createOrReplaceTempView("final_results")
    
    # Cache results
    results_with_names.cache()
    
    print(f"✅ Processed {results_with_names.count()} ranked combinations")
    
    return results_with_names

# Save checkpoints and results
def save_checkpoints(results_df, pagerank_scores, convergence_history):
    """Save all results and checkpoints"""
    
    print("💾 Saving checkpoints and results...")
    
    # Create checkpoints directory
    os.makedirs("checkpoints", exist_ok=True)
    
    # Save results as Parquet
    results_df.write.mode("overwrite").parquet("checkpoints/pagerank_results")
    
    # Save results as JSON
    results_json = results_df.toPandas().to_dict('records')
    with open("checkpoints/pagerank_results.json", "w") as f:
        json.dump(results_json, f, indent=2)
    
    # Save PageRank scores
    pagerank_data = {
        "scores": pagerank_scores.tolist(),
        "convergence_history": convergence_history,
        "timestamp": datetime.now().isoformat(),
        "algorithm_params": {
            "damping_factor": 0.85,
            "max_iterations": 100,
            "tolerance": 1e-6
        }
    }
    
    with open("checkpoints/pagerank_scores.json", "w") as f:
        json.dump(pagerank_data, f, indent=2)
    
    print("✅ All checkpoints saved successfully")

# Visualization functions
def create_visualizations(results_df, convergence_history):
    """Create comprehensive visualizations"""
    
    print("📊 Creating visualizations...")
    
    # Convert to Pandas for plotting
    results_pd = results_df.toPandas()
    
    fig = plt.figure(figsize=(20, 16))
    
    # 1. Top 15 PageRank Scores
    ax1 = plt.subplot(3, 3, 1)
    top_15 = results_pd.head(15)
    bars = ax1.barh(range(len(top_15)), top_15['pagerank_score'], color='skyblue')
    ax1.set_yticks(range(len(top_15)))
    ax1.set_yticklabels([f"{row['vendor_name']}\n{row['item_type_name']}" 
                        for _, row in top_15.iterrows()], fontsize=8)
    ax1.set_xlabel('PageRank Score')
    ax1.set_title('Top 15 Vendor-ItemType Combinations by PageRank')
    ax1.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax1.text(width + 0.0001, bar.get_y() + bar.get_height()/2, 
                f'{width:.4f}', ha='left', va='center', fontsize=7)
    
    # 2. PageRank vs Popularity Score Scatter
    ax2 = plt.subplot(3, 3, 2)
    scatter = ax2.scatter(results_pd['popularity_score'], results_pd['pagerank_score'], 
                         c=results_pd['rank'], cmap='viridis_r', alpha=0.7)
    ax2.set_xlabel('Popularity Score')
    ax2.set_ylabel('PageRank Score')
    ax2.set_title('PageRank vs Popularity Score')
    plt.colorbar(scatter, ax=ax2, label='Rank')
    
    # 3. Convergence History
    ax3 = plt.subplot(3, 3, 3)
    ax3.semilogy(convergence_history)
    ax3.set_xlabel('Iteration')
    ax3.set_ylabel('L1 Difference (log scale)')
    ax3.set_title('PageRank Convergence')
    ax3.grid(True, alpha=0.3)
    
    # 4. Distribution of PageRank Scores
    ax4 = plt.subplot(3, 3, 4)
    ax4.hist(results_pd['pagerank_score'], bins=30, alpha=0.7, color='lightcoral')
    ax4.set_xlabel('PageRank Score')
    ax4.set_ylabel('Frequency')
    ax4.set_title('Distribution of PageRank Scores')
    ax4.grid(axis='y', alpha=0.3)
    
    # 5. Top Vendors by Average PageRank
    ax5 = plt.subplot(3, 3, 5)
    vendor_avg = results_pd.groupby('vendor_name')['pagerank_score'].mean().sort_values(ascending=False)
    bars = ax5.bar(range(len(vendor_avg)), vendor_avg.values, color='lightgreen')
    ax5.set_xticks(range(len(vendor_avg)))
    ax5.set_xticklabels(vendor_avg.index, rotation=45, ha='right')
    ax5.set_ylabel('Average PageRank Score')
    ax5.set_title('Vendors by Average PageRank Score')
    ax5.grid(axis='y', alpha=0.3)
    
    # 6. Top Item Types by Average PageRank
    ax6 = plt.subplot(3, 3, 6)
    itemtype_avg = results_pd.groupby('item_type_name')['pagerank_score'].mean().sort_values(ascending=False)
    bars = ax6.bar(range(len(itemtype_avg)), itemtype_avg.values, color='orange')
    ax6.set_xticks(range(len(itemtype_avg)))
    ax6.set_xticklabels(itemtype_avg.index, rotation=45, ha='right')
    ax6.set_ylabel('Average PageRank Score')
    ax6.set_title('Item Types by Average PageRank Score')
    ax6.grid(axis='y', alpha=0.3)
    
    # 7. Revenue vs PageRank
    ax7 = plt.subplot(3, 3, 7)
    ax7.scatter(results_pd['total_revenue'], results_pd['pagerank_score'], alpha=0.6, color='purple')
    ax7.set_xlabel('Total Revenue')
    ax7.set_ylabel('PageRank Score')
    ax7.set_title('Revenue vs PageRank Score')
    ax7.grid(True, alpha=0.3)
    
    # 8. Heatmap of Vendor-ItemType Matrix (top vendors and item types)
    ax8 = plt.subplot(3, 3, 8)
    top_vendors = results_pd.groupby('vendor_name')['pagerank_score'].sum().nlargest(5).index
    top_itemtypes = results_pd.groupby('item_type_name')['pagerank_score'].sum().nlargest(7).index
    
    heatmap_data = results_pd[results_pd['vendor_name'].isin(top_vendors) & 
                             results_pd['item_type_name'].isin(top_itemtypes)]
    
    pivot_data = heatmap_data.pivot_table(values='pagerank_score', 
                                         index='vendor_name', 
                                         columns='item_type_name', 
                                         fill_value=0)
    
    sns.heatmap(pivot_data, annot=True, fmt='.4f', cmap='YlOrRd', ax=ax8)
    ax8.set_title('PageRank Heatmap (Top Vendors × Item Types)')
    
    ax9 = plt.subplot(3, 3, 9)
    rank_bins = [1, 5, 10, 20, 50, 100, len(results_pd)]
    rank_labels = ['Top 5', '6-10', '11-20', '21-50', '51-100', '100+']
    rank_counts = []
    
    for i in range(len(rank_bins)-1):
        count = len(results_pd[(results_pd['rank'] >= rank_bins[i]) & 
                              (results_pd['rank'] < rank_bins[i+1])])
        rank_counts.append(count)
    
    ax9.pie(rank_counts, labels=rank_labels, autopct='%1.1f%%', startangle=90)
    ax9.set_title('Distribution by Rank Tiers')
    
    plt.tight_layout()
    plt.savefig('checkpoints/pagerank_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("Visualizations created and saved")

def main():    
    print("Starting Vendor-ItemType PageRank Analysis")
    print("=" * 60)
    
    spark = initialize_spark()
    
    try:
        vendors_df, items_df, item_types_df, order_lines_df, ratings_df = load_data(spark)        
        parquet_path = "checkpoints/preprocessed_data"

        def parquet_exists(path):
            return any(f.endswith(".parquet") for f in os.listdir(path)) if os.path.exists(path) else False

        if parquet_exists(parquet_path):
            metrics_df = spark.read.parquet(parquet_path)
            metrics_df.createOrReplaceTempView("metrics")
            print("Parquet files loaded successfully.\n")
            metrics_df.printSchema()
            metrics_df.show(5)
        else:
            print("Here")    
            metrics_df = preprocess_data(spark, vendors_df, items_df, item_types_df, 
                                    order_lines_df, ratings_df)
            metrics_df.createOrReplaceTempView("metrics")
        
        transition_matrix, nodes, node_to_idx = build_transition_matrix(spark, metrics_df)
        pagerank_scores, convergence_history = pagerank_power_method(transition_matrix)
        results_df = process_results(spark, pagerank_scores, nodes, metrics_df)
        save_checkpoints(results_df, pagerank_scores, convergence_history)
        create_visualizations(results_df, convergence_history)
        
        print("\n🏆 TOP 10 RANKED VENDOR-ITEMTYPE COMBINATIONS:")
        print("=" * 80)
        
        top_10 = results_df.orderBy(col('pagerank_score').desc()).limit(10).collect()
        
        for i, row in enumerate(top_10, 1):
            print(f"{i:2d}. {row.vendor_name} - {row.item_type_name}")
            print(f"    PageRank Score: {row.pagerank_score:.6f}")
            print(f"    Popularity Score: {row.popularity_score:.4f}")
            print(f"    Total Orders: {row.total_orders}, Revenue: ${row.total_revenue:.2f}")
            print(f"    Customer Rating: {row.avg_customer_rating:.2f}/5.0")
            print("-" * 60)
        
        # Summary statistics
        print("\n📈 SUMMARY STATISTICS:")
        print("=" * 40)
        
        total_combinations = results_df.count()
        avg_pagerank = results_df.agg({"pagerank_score": "avg"}).collect()[0][0]
        max_pagerank = results_df.agg({"pagerank_score": "max"}).collect()[0][0]
        min_pagerank = results_df.agg({"pagerank_score": "min"}).collect()[0][0]
        
        print(f"Total Combinations Analyzed: {total_combinations}")
        print(f"Average PageRank Score: {avg_pagerank:.6f}")
        print(f"Maximum PageRank Score: {max_pagerank:.6f}")
        print(f"Minimum PageRank Score: {min_pagerank:.6f}")
        print(f"Convergence Iterations: {len(convergence_history)}")
        
        print("\n✅ Analysis completed successfully!")
        print("📁 Results saved in 'checkpoints/' directory")
        print("📊 Visualizations saved as 'pagerank_analysis.png'")
        
    except Exception as e:
        print(e)
        
main()