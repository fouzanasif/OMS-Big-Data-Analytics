import numpy as np
import socket
import json
import time
import logging
import smtplib
import os
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import hashlib
import random
import math
import sqlite3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HyperLogLog:
    """HyperLogLog implementation for distinct counting"""
    
    def __init__(self, precision: int = 20):
        self.precision = precision
        self.m = 2 ** precision
        self.buckets = [0] * self.m
        
    def add(self, value: Any):
        hash_val = int(hashlib.md5(str(value).encode()).hexdigest(), 16)
        j = hash_val & ((1 << self.precision) - 1)
        w = hash_val >> self.precision

        leading_zeros = self._leading_zeros(w) + 1
        before = self.buckets[j]
        self.buckets[j] = np.max([self.buckets[j], leading_zeros])

    
    def _leading_zeros(self, w: int) -> int:
        return (w.bit_length() ^ 64) if w != 0 else 64

    
    def estimate(self) -> int:
        """Estimate the cardinality"""
        # print("[DEBUG] Non-zero buckets:", np.sum(np.array(1 for b in self.customer_hll.buckets if b > 0)))
        raw_estimate = self._alpha_m() * (self.m ** 2) / np.sum(2.0 ** (-x) for x in self.buckets)
        
        # Apply small range correction
        if raw_estimate <= 2.5 * self.m:
            zeros = self.buckets.count(0)
            if zeros != 0:
                return int(self.m * math.log(self.m / zeros))
        
        return int(raw_estimate)
    
    def _alpha_m(self) -> float:
        """Alpha constant for bias correction"""
        if self.m >= 128:
            return 0.7213 / (1 + 1.079 / self.m)
        elif self.m >= 64:
            return 0.709
        elif self.m >= 32:
            return 0.697
        else:
            return 0.5

class DGIMBucket:
    """DGIM Bucket for counting 1s in a sliding window"""
    
    def __init__(self, size: int, timestamp: int):
        self.size = size
        self.timestamp = timestamp

class DGIM:
    """DGIM algorithm for counting events in sliding window"""
    
    def __init__(self, window_size: int):
        self.window_size = window_size
        self.buckets = []
        self.current_time = 0
    
    def add_bit(self, bit: int):
        """Add a new bit to the stream"""
        self.current_time += 1
        
        if bit == 1:
            # Add new bucket of size 1
            self.buckets.append(DGIMBucket(1, self.current_time))
            self._merge_buckets()
        
        self._expire_buckets()
    
    def _merge_buckets(self):
        """Merge buckets to maintain DGIM invariant"""
        size_counts = {}
        
        for bucket in self.buckets:
            if bucket.size not in size_counts:
                size_counts[bucket.size] = []
            size_counts[bucket.size].append(bucket)
        
        # Merge if more than 2 buckets of same size
        for size, bucket_list in size_counts.items():
            while len(bucket_list) > 2:
                # Remove oldest two buckets and add merged bucket
                bucket_list.sort(key=lambda x: x.timestamp)
                old1 = bucket_list.pop(0)
                old2 = bucket_list.pop(0)
                
                merged = DGIMBucket(size * 2, old2.timestamp)
                bucket_list.append(merged)
                
                self.buckets = [b for b in self.buckets if b != old1 and b != old2]
                self.buckets.append(merged)
    
    def _expire_buckets(self):
        """Remove buckets outside the window"""
        cutoff_time = self.current_time - self.window_size
        self.buckets = [b for b in self.buckets if b.timestamp > cutoff_time]
    
    def count(self) -> int:
        """Estimate count of 1s in current window"""
        if not self.buckets:
            return 0
        
        total = np.sum(b.size for b in self.buckets[1:])  # All but oldest
        if self.buckets:
            total += self.buckets[0].size // 2  # Half of oldest bucket
        
        return total

class BloomFilter:
    """Bloom filter for membership testing"""
    
    def __init__(self, capacity: int, error_rate: float = 0.01):
        self.capacity = capacity
        self.error_rate = error_rate
        self.bit_array_size = self._get_size()
        self.hash_count = self._get_hash_count()
        self.bit_array = [0] * self.bit_array_size
    
    def _get_size(self) -> int:
        """Calculate optimal bit array size"""
        m = -(self.capacity * math.log(self.error_rate)) / (math.log(2) ** 2)
        return int(m)
    
    def _get_hash_count(self) -> int:
        """Calculate optimal number of hash functions"""
        k = (self.bit_array_size / self.capacity) * math.log(2)
        return int(k)
    
    def _hash(self, item: Any, seed: int) -> int:
        """Hash function with seed"""
        hash_val = hashlib.md5((str(item) + str(seed)).encode()).hexdigest()
        return int(hash_val, 16) % self.bit_array_size
    
    def add(self, item: Any):
        """Add item to bloom filter"""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            self.bit_array[index] = 1
    
    def contains(self, item: Any) -> bool:
        """Check if item might be in the set"""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            if self.bit_array[index] == 0:
                return False
        return True

class IBLTCell:
    """Cell for Invertible Bloom Lookup Table"""
    
    def __init__(self):
        self.count = 0
        self.key_sum = 0
        self.value_sum = 0
        self.hash_sum = 0

def int_md5(x):
    return int(hashlib.md5(str(x).encode()).hexdigest(), 16)

class InvertibleBloomLookupTable:
    """IBLT for set reconciliation and difference detection"""
    
    def __init__(self, size: int, hash_count: int = 3):
        self.size = size
        self.hash_count = hash_count
        self.cells = [IBLTCell() for _ in range(size)]
    
    def _hash_functions(self, key: Any) -> List[int]:
        """Generate multiple hash values for a key"""
        hashes = []
        for i in range(self.hash_count):
            hash_val = hashlib.md5((str(key) + str(i)).encode()).hexdigest()
            hashes.append(int(hash_val, 16) % self.size)
        return hashes
    
    def _key_hash(self, key: Any) -> int:
        """Hash function for key integrity"""
        return int(hashlib.md5(str(key).encode()).hexdigest(), 16)
    
    def insert(self, key: Any, value: Any):
        """Insert key-value pair"""
        key_hash = self._key_hash(key)
        positions = self._hash_functions(key)
        
        for pos in positions:
            cell = self.cells[pos]
            cell.count += 1
            stable_key_hash = int_md5(key)
            stable_value_hash = int_md5(value)
            cell.key_sum ^= stable_key_hash
            cell.value_sum ^= stable_value_hash
            cell.hash_sum ^= stable_key_hash
    
    def delete(self, key: Any, value: Any):
        """Delete key-value pair"""
        key_hash = self._key_hash(key)
        positions = self._hash_functions(key)
        
        for pos in positions:
            cell = self.cells[pos]
            cell.count -= 1
            cell.key_sum ^= hash(key)
            cell.value_sum ^= hash(value)
            cell.hash_sum ^= key_hash
    
    def get_differences(self) -> List[tuple]:
        """Extract differences (used for set reconciliation)"""
        differences = []
        changed = True
        
        while changed:
            changed = False
            for i, cell in enumerate(self.cells):
                if cell.count == 1:
                    # Pure cell - can extract the key-value pair
                    key_candidate = cell.key_sum
                    value_candidate = cell.value_sum
                    
                    # Verify integrity
                    if self._key_hash(key_candidate) == cell.hash_sum:
                        differences.append((key_candidate, value_candidate))
                        
                        # Remove this entry from other cells
                        positions = self._hash_functions(key_candidate)
                        for pos in positions:
                            self.cells[pos].count -= 1
                            self.cells[pos].key_sum ^= key_candidate
                            self.cells[pos].value_sum ^= value_candidate
                            self.cells[pos].hash_sum ^= self._key_hash(key_candidate)
                        
                        changed = True
                        break
        
        return differences

class StreamingProcessor:
    """Main streaming processor with all algorithms"""
    
    def __init__(self, db_path: str):
        self.not_mailed = True
        self.db_path = db_path
        self.window_size_hours = 1
        self.window_size_seconds = self.window_size_hours * 3600
        self.checkpoint_dir = "streaming_checkpoints"
        self.checkpoint_file = os.path.join(self.checkpoint_dir, "processor_state.pkl")
        
        # Initialize algorithms
        self.customer_hll = HyperLogLog(precision=20)
        self.item_hll = HyperLogLog(precision=20)
        self.event_counters = {}  # event_name -> DGIM
        self.focus_items_bloom = BloomFilter(capacity=10000)
        self.order_iblt = InvertibleBloomLookupTable(size=1000)
        
        # Sliding window for reservoir sampling
        self.window_samples = []
        self.window_start_time = None
        
        # Email configuration
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.email_user = "k191345@nu.edu.pk"  # Configure this
        self.email_password = ""  # Configure this
        self.target_email = "mfouzan.asif@gmail.com"
        
        # Create checkpoint directory
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Load focus items from database
        self._load_focus_items()
        
        # Restore from checkpoint if exists
        self._restore_checkpoint()
    
    def _load_focus_items(self):
        """Load items that should be focused (add to bloom filter)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Assuming items table exists - focus on high-value items
            # This is a placeholder query - adjust based on your items table schema
            cursor.execute("SELECT id FROM items LIMIT 1000")  # Focus on first 1000 items
            
            for row in cursor.fetchall():
                item_id = row[0]
                self.focus_items_bloom.add(item_id)
            
            conn.close()
            logger.info("Loaded focus items into Bloom filter")
            
        except Exception as e:
            logger.warning(f"Could not load focus items: {e}")
    
    def _save_checkpoint(self):
        """Save current state to checkpoint"""
        state = {
            'customer_hll': self.customer_hll,
            'item_hll': self.item_hll,
            'event_counters': self.event_counters,
            'focus_items_bloom': self.focus_items_bloom,
            'order_iblt': self.order_iblt,
            'window_samples': self.window_samples,
            'window_start_time': self.window_start_time,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(state, f)
        
        logger.info("Checkpoint saved successfully")
    
    def _restore_checkpoint(self):
        """Restore state from checkpoint"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    state = pickle.load(f)
                
                self.customer_hll = state.get('customer_hll', HyperLogLog())
                self.item_hll = state.get('item_hll', HyperLogLog())
                self.event_counters = state.get('event_counters', {})
                self.focus_items_bloom = state.get('focus_items_bloom', BloomFilter(10000))
                self.order_iblt = state.get('order_iblt', InvertibleBloomLookupTable(1000))
                self.window_samples = state.get('window_samples', [])
                self.window_start_time = state.get('window_start_time')
                
                logger.info(f"Checkpoint restored from {state.get('timestamp', 'unknown time')}")
                
            except Exception as e:
                logger.error(f"Failed to restore checkpoint: {e}")
    
    def _should_sample(self, record: Dict[str, Any]) -> bool:
        current_time = datetime.fromisoformat(record['created_at'].replace('Z', '+00:00'))
        
        if self.window_start_time is None:
            self.window_start_time = current_time
        
        # Check if record is within the time window
        time_diff = (current_time - self.window_start_time).total_seconds()
        
        if time_diff > self.window_size_seconds:
            # Slide the window
            cutoff_time = current_time - timedelta(seconds=self.window_size_seconds)
            self.window_samples = [s for s in self.window_samples 
                                 if datetime.fromisoformat(s['created_at'].replace('Z', '+00:00')) > cutoff_time]
            self.window_start_time = cutoff_time
        
        # Reservoir sampling: always accept if window not full
        max_window_size = 50000  # Optimal window size
        
        if len(self.window_samples) < max_window_size:
            return True
        else:
            # Replace with probability 1/n where n is number of items seen
            return random.random() < (max_window_size / (max_window_size + 1))
    
    def _send_alert_email(self, down_count: int):
        """Send email alert when DOWN events exceed threshold"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_user
            msg['To'] = self.target_email
            msg['Subject'] = "ALERT: High DOWN Event Count Detected"
            
            body = f"""
            ALERT: Order Management System
            
            The count of DOWN events has exceeded the threshold of 1000.
            Current DOWN event count: {down_count}
            
            Timestamp: {datetime.now().isoformat()}
            
            Please investigate the system immediately.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_user, self.email_password)
            text = msg.as_string()
            server.sendmail(self.email_user, self.target_email, text)
            server.quit()
            
            logger.info(f"Alert email sent for DOWN count: {down_count}")
            
        except Exception as e:
            logger.info(f"Alert email not sent for DOWN count: {down_count}")
            # logger.error(f"Failed to send alert email: {e}")
    
    def process_batch(self, records: List[Dict[str, Any]]):
        """Process a batch of records"""
        for record in records:
            # Sliding window sampling
            if self._should_sample(record):
                self.window_samples.append(record)
                # Update HyperLogLog counters
                if record.get('customer_id'):
                    self.customer_hll.add(record['customer_id'])
                if record.get('item_id'):
                    self.item_hll.add(record['item_id'])
                    # Check if item is in focus using Bloom filter
                    if self.focus_items_bloom.contains(record['item_id']):
                        logger.info(f"Focus item detected: {record['item_id']}")
                # Update DGIM for event counting
                event_name = record.get('event_name', 'UNKNOWN')
                if event_name not in self.event_counters:
                    self.event_counters[event_name] = DGIM(window_size=3600)  # 1 hour window
                
                self.event_counters[event_name].add_bit(1)
                
                # Check for DOWN events threshold
                if event_name == 'DOWN':
                    down_count = self.event_counters['DOWN'].count()
                    if down_count > 1000 and self.not_mailed:
                        self._send_alert_email(down_count)
                        self.not_mailed = False
                    if down_count < 1000:
                        self.not_mailed = True
                
                # IBLT for order tracking (example use case)
                if record.get('item_id') and record.get('customer_id'):
                    order_key = f"{record['customer_id']}_{record['item_id']}"
                    self.order_iblt.insert(order_key, record.get('event_name', ''))
                
        # Log current statistics
        self._log_statistics()
        
        # Save checkpoint periodically
        if len(self.window_samples) % 10000 == 0:
            self._save_checkpoint()
    
    def _log_statistics(self):
        """Log current streaming statistics"""
        customer_count = self.customer_hll.estimate()
        item_count = self.item_hll.estimate()
        
        logger.info(f"Window Statistics:")
        logger.info(f"  - Distinct customers: {customer_count}")
        logger.info(f"  - Distinct items: {item_count}")
        logger.info(f"  - Window samples: {len(self.window_samples)}")
        
        for event_name, dgim in self.event_counters.items():
            event_count = dgim.count()
            logger.info(f"  - {event_name} events: {event_count}")
            self.event_counters[event_name] = DGIM(window_size=3600)
        
        self.customer_hll = HyperLogLog(precision=20)
        self.item_hll = HyperLogLog(precision=20)
        
        

class SocketServer:
    """Socket server to receive data from producer"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999, db_path: str = None):
        self.host = host
        self.port = port
        self.processor = StreamingProcessor(db_path)
        
    def start(self):
        """Start the socket server"""
        logger.info(f"Starting socket server on {self.host}:{self.port}")
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen(1)
            
            logger.info("Server listening for connections...")
            
            while True:
                try:
                    client_socket, address = server_socket.accept()
                    logger.info(f"Connection from {address}")
                    
                    with client_socket:
                        data = b''
                        while True:
                            chunk = client_socket.recv(4096)
                            if not chunk:
                                break
                            data += chunk
                            
                            # Check for complete message (ends with newline)
                            if b'\n' in data:
                                message = data.decode('utf-8').strip()
                                
                                try:
                                    batch_data = json.loads(message)
                                    records = batch_data.get('records', [])
                                    
                                    logger.info(f"Received batch with {len(records)} records")
                                    
                                    # Process the batch
                                    self.processor.process_batch(records)
                                    
                                    # Send acknowledgment
                                    client_socket.sendall(b'ACK')
                                    
                                except json.JSONDecodeError as e:
                                    logger.error(f"JSON decode error: {e}")
                                    client_socket.sendall(b'NACK')
                                
                                break
                                
                except KeyboardInterrupt:
                    logger.info("Server stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Server error: {e}")
                    break

if __name__ == "__main__":
    # Configuration
    DATABASE_PATH = "OMS.db"  # Update with your database path
    HOST = "localhost"
    PORT = 9999
    
    server = SocketServer(HOST, PORT, DATABASE_PATH)
    server.start()