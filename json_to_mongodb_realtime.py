#!/usr/bin/env python3
"""
Real-Time JSON to MongoDB Transfer Script

Monitors a directory for new JSON files containing ML predictions
and immediately transfers them to MongoDB as they are created.
"""

import json
import logging
import time
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from typing import List, Dict

import pymongo
from pymongo import MongoClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mongodb_transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class JSONFileHandler(FileSystemEventHandler):
    """Handles file system events for JSON files"""
    
    def __init__(self, file_queue: Queue):
        super().__init__()
        self.file_queue = file_queue
        self.processed_files = set()
    
    def on_created(self, event):
        """Process new JSON files"""
        self._handle_file_event(event, "created")
    
    def on_modified(self, event):
        """Process modified JSON files"""
        self._handle_file_event(event, "modified")
    
    def _handle_file_event(self, event, event_type):
        """Common logic for file events"""
        if event.is_directory or not event.src_path.endswith('.json'):
            return
            
        file_path = Path(event.src_path)
        if file_path not in self.processed_files:
            logger.info(f"JSON file {event_type}: {file_path.name}")
            self.file_queue.put(str(file_path))
            self.processed_files.add(file_path)


class RealTimeJSONToMongoDB:
    """Real-time JSON to MongoDB transfer utility"""
    
    def __init__(self, mongodb_uri="mongodb://localhost:27017", 
                 database="cti_db", collection="predictions", 
                 watch_dir="./predictions_output"):
        
        self.mongodb_uri = mongodb_uri
        self.database_name = database
        self.collection_name = collection
        self.watch_directory = Path(watch_dir)
        self.watch_directory.mkdir(exist_ok=True)
        
        # Threading components
        self.file_queue = Queue()
        self.stop_event = threading.Event()
        self.processing_thread = None
        
        # MongoDB components
        self.client = None
        self.collection = None
        
        # File monitoring
        self.observer = Observer()
        self.file_handler = JSONFileHandler(self.file_queue)
        
        # Statistics
        self.files_processed = 0
        self.records_inserted = 0
        
        logger.info(f"Monitoring: {self.watch_directory}")
        logger.info(f"MongoDB: {database}.{collection}")
    
    def connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.client.admin.command('ping')  # Test connection
            
            db = self.client[self.database_name]
            self.collection = db[self.collection_name]
            
            logger.info("MongoDB connected successfully")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
    
    def read_json_file(self, file_path: str) -> List[Dict]:
        """Read JSON file with retry logic"""
        file_path = Path(file_path)
        
        # Retry up to 3 times (handles files still being written)
        for attempt in range(3):
            try:
                time.sleep(0.2)  # Wait for file write completion
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                
                if not content:
                    if attempt < 2:
                        logger.warning(f"Empty file, retrying: {file_path.name}")
                        continue
                    return []
                
                records = []
                for line_num, line in enumerate(content.split('\n'), 1):
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            # Add metadata
                            record.update({
                                '_imported_at': datetime.utcnow(),
                                '_source_file': file_path.name,
                                '_line_number': line_num
                            })
                            records.append(record)
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON error in {file_path.name}:{line_num} - {e}")
                
                logger.info(f"Read {len(records)} records from {file_path.name}")
                return records
                
            except (IOError, OSError) as e:
                if attempt < 2:
                    logger.warning(f"Read error, retrying: {e}")
                else:
                    logger.error(f"Failed to read {file_path}: {e}")
                    return []
        
        return []
    
    def insert_to_mongodb(self, records: List[Dict]) -> bool:
        """Insert records to MongoDB"""
        if not records:
            return True
        
        try:
            result = self.collection.insert_many(records, ordered=False)
            count = len(result.inserted_ids)
            self.records_inserted += count
            logger.info(f"Inserted {count} records to MongoDB")
            return True
            
        except pymongo.errors.BulkWriteError as e:
            # Handle partial success
            count = e.details.get('nInserted', 0)
            self.records_inserted += count
            logger.warning(f"Partial insert: {count} records, {len(e.details.get('writeErrors', []))} errors")
            return count > 0
            
        except Exception as e:
            logger.error(f"MongoDB insert failed: {e}")
            return False
    
    def process_files(self):
        """Process files from queue (runs in background thread)"""
        logger.info("File processor started")
        
        while not self.stop_event.is_set():
            try:
                file_path = self.file_queue.get(timeout=1.0)
                
                if file_path is None:  # Shutdown signal
                    break
                
                # Process the file
                records = self.read_json_file(file_path)
                if records and self.insert_to_mongodb(records):
                    self.files_processed += 1
                    logger.info(f"Processed {Path(file_path).name} - "
                              f"Files: {self.files_processed}, Records: {self.records_inserted}")
                
                self.file_queue.task_done()
                
            except Empty:
                continue  # Timeout - check stop_event
            except Exception as e:
                logger.error(f"Processing error: {e}")
                if not self.file_queue.empty():
                    self.file_queue.task_done()
        
        logger.info("File processor stopped")
    
    def process_existing_files(self):
        """Process any existing JSON files"""
        existing_files = list(self.watch_directory.glob("*.json"))
        if existing_files:
            logger.info(f"Processing {len(existing_files)} existing files")
            for file_path in existing_files:
                self.file_queue.put(str(file_path))
                self.file_handler.processed_files.add(file_path)
    
    def start(self):
        """Start monitoring"""
        if not self.connect_mongodb():
            return False
        
        logger.info("Starting real-time monitoring...")
        
        # Start file processor thread
        self.processing_thread = threading.Thread(target=self.process_files, daemon=True)
        self.processing_thread.start()
        
        # Start file system monitoring
        self.observer.schedule(self.file_handler, str(self.watch_directory), recursive=False)
        self.observer.start()
        
        # Process existing files
        self.process_existing_files()
        
        logger.info("Monitoring started. Press Ctrl+C to stop.")
        return True
    
    def stop(self):
        """Stop monitoring gracefully"""
        logger.info("Stopping monitoring...")
        
        self.stop_event.set()
        
        # Stop file system observer
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join(timeout=5)
        
        # Finish processing remaining files
        if not self.file_queue.empty():
            logger.info("Processing remaining files...")
            self.file_queue.join()
        
        # Stop processor thread
        self.file_queue.put(None)
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=10)
        
        # Close MongoDB
        if self.client:
            self.client.close()
        
        logger.info(f"Stopped. Processed {self.files_processed} files, {self.records_inserted} records")
    
    def run(self):
        """Main run loop"""
        try:
            if not self.start():
                logger.error("Failed to start monitoring")
                return False
            
            # Keep running until interrupted
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.stop()
        
        return True


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time JSON to MongoDB transfer")
    parser.add_argument("--mongodb-uri", default="mongodb://localhost:27017",
                       help="MongoDB URI")
    parser.add_argument("--database", default="cti_db",
                       help="Database name")
    parser.add_argument("--collection", default="predictions",
                       help="Collection name")
    parser.add_argument("--watch-dir", default="./predictions_output",
                       help="Directory to monitor")
    
    args = parser.parse_args()
    
    logger.info("=== Real-Time JSON to MongoDB Transfer ===")
    
    transfer = RealTimeJSONToMongoDB(
        mongodb_uri=args.mongodb_uri,
        database=args.database,
        collection=args.collection,
        watch_dir=args.watch_dir
    )
    
    success = transfer.run()
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
