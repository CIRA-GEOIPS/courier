import os
import sys
import time
import signal
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Add shared modules to path
sys.path.append('/app')

from shared.rabbitmq_client import RabbitMQClient
from shared.database import DatabaseClient
from shared.models import FileNotification

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FileWatcherHandler(FileSystemEventHandler):
    def __init__(self, rabbitmq_client: RabbitMQClient, database_client: DatabaseClient, service_id: str):
        self.rabbitmq = rabbitmq_client
        self.database = database_client
        self.service_id = service_id
        
        # File patterns for CWC processing
        self.file_patterns = {
            'cwc': re.compile(r'clavrx_OR_ABI-L1b-RadF-M6C01_G16_s(\d+)\.CWC\.h5$'),
            'clavrx': re.compile(r'clavrx_OR_ABI-L1b-RadF-M6C01_G16_s(\d+)\.level2\.hdf$')
        }

    def on_created(self, event):
        if event.is_directory:
            return
        
        self.process_file(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        
        self.process_file(event.dest_path)

    def process_file(self, file_path: str):
        """Process detected file and publish notification"""
        try:
            filename = os.path.basename(file_path)
            logger.info(f"Detected new file: {filename}")
            
            # Extract timestamp from filename
            timestamp = self.extract_timestamp(filename)
            if not timestamp:
                logger.warning(f"Could not extract timestamp from {filename}")
                return

            # Create file notification
            notification = FileNotification(
                filename=filename,
                file_path=file_path,
                timestamp=timestamp,
                watcher_id=self.service_id,
                detected_at=datetime.utcnow().isoformat(),
                batch_id=f"batch_{timestamp}"
            )

            # Publish to RabbitMQ
            self.rabbitmq.publish("file_notifications", notification.model_dump())

            logger.info(f"File notification published for {filename} with timestamp {timestamp}")

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

    def extract_timestamp(self, filename: str) -> str:
        """Extract timestamp from filename using regex patterns"""
        for file_type, pattern in self.file_patterns.items():
            match = pattern.search(filename)
            if match:
                return match.group(1)
        return None

class WatcherService:
    def __init__(self):
        self.service_id = os.environ.get('SERVICE_ID', f"watcher-service-{uuid.uuid4().hex[:8]}")
        self.rabbitmq_url = os.environ.get('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.database_url = os.environ.get('DATABASE_URL', 'postgresql://admin:admin@localhost:5432/geoips_system')
        self.watch_directory = os.environ.get('WATCH_DIRECTORY', '/data/input')
        
        # Initialize clients
        self.rabbitmq = RabbitMQClient(self.rabbitmq_url)
        
        # Setup file watcher
        self.observer = Observer()
        self.handler = FileWatcherHandler(self.rabbitmq, None, self.service_id)
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        
        self.running = True

    def start(self):
        """Start the watcher service"""
        logger.info(f"Starting Watcher Service {self.service_id}")
        logger.info(f"Watching directory: {self.watch_directory}")
        
        # Ensure watch directory exists
        Path(self.watch_directory).mkdir(parents=True, exist_ok=True)
        
        # Start file system observer
        self.observer.schedule(self.handler, self.watch_directory, recursive=False)
        self.observer.start()
        
        logger.info("Watcher service started successfully")
        
        # Process any existing files
        self.process_existing_files()
        
        # Keep service running
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.cleanup()

    def process_existing_files(self):
        """Process files that already exist in the watch directory"""
        try:
            watch_path = Path(self.watch_directory)
            for file_path in watch_path.iterdir():
                if file_path.is_file():
                    logger.info(f"Processing existing file: {file_path.name}")
                    self.handler.process_file(str(file_path))
        except Exception as e:
            logger.error(f"Error processing existing files: {e}")

    def graceful_shutdown(self, signal_num: int, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signal_num}, shutting down gracefully...")
        self.running = False

    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
        
        self.rabbitmq.close()
        logger.info("Watcher service stopped")

if __name__ == "__main__":
    service = WatcherService()
    service.start()
