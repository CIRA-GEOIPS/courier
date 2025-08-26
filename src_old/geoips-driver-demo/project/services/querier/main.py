import os
import sys
import json
import signal
import logging
import uuid
import time
import pika
from datetime import datetime
from collections import defaultdict
from typing import Dict, Set

# Add shared modules to path
sys.path.append('/app')

from shared.database import DatabaseClient
from shared.models import FileNotification, JobSubmission, JobStatus

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QuerierService:
    def __init__(self):
        self.service_id = os.environ.get('SERVICE_ID', f"querier-service-{uuid.uuid4().hex[:8]}")
        self.rabbitmq_url = os.environ.get('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.database_url = os.environ.get('DATABASE_URL', 'postgresql://admin:admin@localhost:5432/geoips_system')
        
        # Initialize database client
        self.database = DatabaseClient(self.database_url)
        
        # File tracking
        self.file_groups: Dict[str, Dict[str, str]] = defaultdict(dict)
        self.processed_jobs: Set[str] = set()
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        
        self.running = True

    def create_rabbitmq_connection(self):
        """Create a new RabbitMQ connection"""
        parameters = pika.URLParameters(self.rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare queues
        queues = ["file_notifications", "job_submissions", "job_status", "output_files"]
        for queue_name in queues:
            channel.queue_declare(queue=queue_name, durable=True)
        
        return connection, channel

    def start(self):
        """Start the querier service"""
        logger.info(f"Starting Querier Service {self.service_id}")
        
        # Main processing loop
        while self.running:
            try:
                # Process file notifications
                self.process_file_notifications()
                
                # Check for complete file sets
                self.check_complete_file_sets()
                
                # Process job status updates
                self.process_job_status_updates()
                
                # Short sleep to prevent busy waiting
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)  # Wait before retrying
        
        self.cleanup()

    def process_file_notifications(self):
        """Process pending file notifications"""
        try:
            connection, channel = self.create_rabbitmq_connection()
            
            # Check for messages without blocking
            method_frame, header_frame, body = channel.basic_get(queue='file_notifications', auto_ack=False)
            
            if method_frame:
                try:
                    message = json.loads(body)
                    notification = FileNotification(**message)
                    self.process_file_notification(notification)
                    channel.basic_ack(method_frame.delivery_tag)
                    logger.info(f"Processed file notification: {notification.filename}")
                except Exception as e:
                    logger.error(f"Error processing file notification: {e}")
                    channel.basic_nack(method_frame.delivery_tag, requeue=False)
            
            connection.close()
            
        except Exception as e:
            logger.error(f"Error processing file notifications: {e}")

    def process_job_status_updates(self):
        """Process pending job status updates"""
        try:
            connection, channel = self.create_rabbitmq_connection()
            
            # Check for messages without blocking
            method_frame, header_frame, body = channel.basic_get(queue='job_status', auto_ack=False)
            
            if method_frame:
                try:
                    message = json.loads(body)
                    status = JobStatus(**message)
                    self.process_job_status(status)
                    channel.basic_ack(method_frame.delivery_tag)
                    logger.info(f"Processed job status: {status.job_id} - {status.status}")
                except Exception as e:
                    logger.error(f"Error processing job status: {e}")
                    channel.basic_nack(method_frame.delivery_tag, requeue=False)
            
            connection.close()
            
        except Exception as e:
            logger.error(f"Error processing job status updates: {e}")

    def process_file_notification(self, notification: FileNotification):
        """Process incoming file notification"""
        timestamp = notification.timestamp
        filename = notification.filename
        
        # Determine file type
        file_type = None
        if filename.endswith('.CWC.h5'):
            file_type = 'cwc'
        elif filename.endswith('.level2.hdf'):
            file_type = 'clavrx'
        
        if not file_type:
            logger.warning(f"Unknown file type for {filename}")
            return
        
        # Add to file groups
        self.file_groups[timestamp][file_type] = filename
        
        file_count = len(self.file_groups[timestamp])
        logger.info(f"Collected {file_count}/2 files for timestamp {timestamp}")

    def process_job_status(self, status: JobStatus):
        """Process job status update"""
        logger.info(f"Job {status.job_id} status: {status.status}")
        
        if status.status == "completed" and status.output_files:
            for output_file in status.output_files:
                logger.info(f"Job {status.job_id} produced output: {output_file}")

    def check_complete_file_sets(self):
        """Check for complete file sets and submit jobs"""
        for timestamp in list(self.file_groups.keys()):
            if self.has_complete_file_set(timestamp):
                job_id = f"cwc_processing_{timestamp}"
                if job_id not in self.processed_jobs:
                    self.submit_job(timestamp)

    def has_complete_file_set(self, timestamp: str) -> bool:
        """Check if we have both CWC and CLAVRX files for timestamp"""
        files = self.file_groups[timestamp]
        return 'cwc' in files and 'clavrx' in files

    def submit_job(self, timestamp: str):
        """Submit job when both files are available"""
        files = self.file_groups[timestamp]
        job_id = f"cwc_processing_{timestamp}"
        
        if job_id in self.processed_jobs:
            return
        
        self.processed_jobs.add(job_id)
        
        try:
            # Create job submission
            job = JobSubmission(
                job_id=job_id,
                cwc_file=files['cwc'],
                clavrx_file=files['clavrx'],
                template="add_cloud_water_content",
                submitted_by=self.service_id,
                submitted_at=datetime.utcnow().isoformat()
            )
            
            # Publish job submission
            connection, channel = self.create_rabbitmq_connection()
            message_body = json.dumps(job.model_dump(), default=str)
            channel.basic_publish(
                exchange='',
                routing_key='job_submissions',
                body=message_body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            connection.close()
            
            logger.info(f"Collected 2/2 files - submitting job {job_id}")
            
        except Exception as e:
            logger.error(f"Error submitting job {job_id}: {e}")

    def graceful_shutdown(self, signal_num: int, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signal_num}, shutting down gracefully...")
        self.running = False

    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        self.database.close()
        logger.info("Querier service stopped")

if __name__ == "__main__":
    service = QuerierService()
    service.start()
