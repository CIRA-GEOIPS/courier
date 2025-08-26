import pika
import json
import logging
import time
from typing import Callable, Dict, Any
from pika.exceptions import AMQPConnectionError, AMQPChannelError

logger = logging.getLogger(__name__)

class RabbitMQClient:
    def __init__(self, rabbitmq_url: str, max_retries: int = 5):
        self.rabbitmq_url = rabbitmq_url
        self.max_retries = max_retries
        self.connection = None
        self.channel = None
        self.queues = {
            "file_notifications": {"durable": True},
            "job_submissions": {"durable": True},
            "job_status": {"durable": True},
            "output_files": {"durable": True}
        }
        self.connect()
        self.setup_queues()

    def connect(self):
        """Establish connection to RabbitMQ with retry logic"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1})")
                parameters = pika.URLParameters(self.rabbitmq_url)
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                logger.info("Successfully connected to RabbitMQ")
                return
            except AMQPConnectionError as e:
                logger.warning(f"Failed to connect to RabbitMQ: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached. Could not connect to RabbitMQ")
                    raise

    def setup_queues(self):
        """Declare all required queues"""
        for queue_name, config in self.queues.items():
            self.channel.queue_declare(queue=queue_name, **config)
            logger.info(f"Queue '{queue_name}' declared")

    def publish(self, queue_name: str, message: Dict[Any, Any]):
        """Publish message to specified queue"""
        try:
            if not self.connection or self.connection.is_closed:
                self.connect()
                self.setup_queues()

            message_body = json.dumps(message, default=str)
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
            )
            logger.info(f"Published message to {queue_name}: {message.get('message_type', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to publish message to {queue_name}: {e}")
            raise

    def consume(self, queue_name: str, callback: Callable):
        """Start consuming messages from specified queue"""
        try:
            if not self.connection or self.connection.is_closed:
                self.connect()
                self.setup_queues()

            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            logger.info(f"Started consuming from {queue_name}")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumption...")
            self.channel.stop_consuming()
        except Exception as e:
            logger.error(f"Error consuming from {queue_name}: {e}")
            raise

    def get_new_connection(self):
        """Get a new independent connection for multiple consumers"""
        parameters = pika.URLParameters(self.rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare queues on new connection
        for queue_name, config in self.queues.items():
            channel.queue_declare(queue=queue_name, **config)
        
        return connection, channel

    def close(self):
        """Close RabbitMQ connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed")
