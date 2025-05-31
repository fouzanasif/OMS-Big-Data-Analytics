import sqlite3
import socket
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseProducer:
    def __init__(self, db_path: str, host: str = 'localhost', port: int = 9999):
        self.db_path = db_path
        self.host = host
        self.port = port
        self.batch_size = 10000
        
    def connect_db(self):
        """Create database connection"""
        return sqlite3.connect(self.db_path)
    
    def get_unprocessed_records(self, conn: sqlite3.Connection, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch unprocessed records in batches"""
        query = """
        SELECT ua.id, ua.customer_id, ua.event_id, ua.status_code, 
               ua.description, ua.item_id, ua.datetime, ua.created_at,
               e.name as event_name
        FROM user_activity ua
        JOIN events e ON ua.event_id = e.id
        WHERE ua.is_processed = 0
        ORDER BY ua.created_at ASC
        LIMIT ?
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (self.batch_size,))
        
        columns = [desc[0] for desc in cursor.description]
        records = []
        
        for row in cursor.fetchall():
            record = dict(zip(columns, row))
            records.append(record)
            
        return records
    
    def mark_as_processed(self, conn: sqlite3.Connection, record_ids: List[int]):
        """Mark records as processed"""
        if not record_ids:
            return
            
        placeholders = ','.join(['?' for _ in record_ids])
        query = f"UPDATE user_activity SET is_processed = 1 WHERE id IN ({placeholders})"
        
        cursor = conn.cursor()
        cursor.execute(query, record_ids)
        conn.commit()
        
        logger.info(f"Marked {len(record_ids)} records as processed")
    
    def send_batch_to_consumer(self, records: List[Dict[str, Any]]) -> bool:
        """Send batch of records to consumer via socket"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.host, self.port))
                
                # Send batch data as JSON
                batch_data = {
                    'timestamp': datetime.now().isoformat(),
                    'batch_size': len(records),
                    'records': records
                }
                
                message = json.dumps(batch_data) + '\n'
                sock.sendall(message.encode('utf-8'))
                
                # Wait for acknowledgment
                response = sock.recv(1024).decode('utf-8')
                if response.strip() == 'ACK':
                    logger.info(f"Successfully sent batch of {len(records)} records")
                    return True
                else:
                    logger.error(f"Unexpected response from consumer: {response}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error sending batch to consumer: {e}")
            return False
    
    def run(self):
        """Main producer loop"""
        logger.info("Starting database producer...")
        
        conn = self.connect_db()
        offset = 0
        
        try:
            while True:
                # Fetch batch of unprocessed records
                records = self.get_unprocessed_records(conn, offset)
                
                if not records:
                    logger.info("No more unprocessed records found. Waiting...")
                    time.sleep(10)  # Wait 10 seconds before checking again
                    offset = 0  # Reset offset
                    continue
                
                logger.info(f"Processing batch of {len(records)} records (offset: {offset})")
                
                # Send batch to consumer
                if self.send_batch_to_consumer(records):
                    # Mark records as processed
                    record_ids = [record['id'] for record in records]
                    self.mark_as_processed(conn, record_ids)
                    
                    if len(records) < self.batch_size:
                        # Last batch, reset offset
                        offset = 0
                    else:
                        offset += self.batch_size
                else:
                    logger.error("Failed to send batch, retrying in 5 seconds...")
                    time.sleep(5)
                
                # Small delay between batches
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    # Configuration
    DATABASE_PATH = "OMS.db"  # Update with your database path
    HOST = "localhost"
    PORT = 9999
    
    producer = DatabaseProducer(DATABASE_PATH, HOST, PORT)
    producer.run()