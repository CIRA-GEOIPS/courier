import os
import sys
import json
import signal
import logging
import uuid
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from jinja2 import Template

# Add shared modules to path
sys.path.append('/app')

from shared.rabbitmq_client import RabbitMQClient
from shared.database import DatabaseClient
from shared.models import JobSubmission, JobStatus, OutputFileNotification

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DispatcherService:
    def __init__(self):
        self.service_id = os.environ.get('SERVICE_ID', f"dispatcher-service-{uuid.uuid4().hex[:8]}")
        self.rabbitmq_url = os.environ.get('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.database_url = os.environ.get('DATABASE_URL', 'postgresql://admin:admin@localhost:5432/geoips_system')
        
        # Initialize clients
        self.rabbitmq = RabbitMQClient(self.rabbitmq_url)
        self.database = DatabaseClient(self.database_url)
        
        # Setup directories
        self.template_dir = "/app/shared/templates"
        self.temp_dir = "/data/temp"
        self.output_dir = "/data/output"
        
        # Ensure directories exist
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        
        self.running = True

    def start(self):
        """Start the dispatcher service"""
        logger.info(f"Starting Dispatcher Service {self.service_id}")
        
        # Start consuming job submissions
        consume_thread = threading.Thread(target=self.consume_job_submissions, daemon=True)
        consume_thread.start()
        
        logger.info("Dispatcher service started successfully")
        
        # Keep service running
        try:
            while self.running:
                threading.Event().wait(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.cleanup()

    def consume_job_submissions(self):
        """Consume job submissions from querier service"""
        def callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                job = JobSubmission(**message)
                self.process_job(job)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error processing job submission: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        logger.info("Starting to consume job submissions")
        self.rabbitmq.consume("job_submissions", callback)

    def process_job(self, job: JobSubmission):
        """Process a job submission"""
        logger.info(f"Executing job {job.job_id}")
        
        # Send job accepted status
        self.publish_job_status(job.job_id, "accepted")
        
        try:
            # Execute the job
            success, output_files, error_message = self.execute_job(job)
            
            if success:
                # Send job completed status
                self.publish_job_status(job.job_id, "completed", output_files=output_files)
                
                # Publish output file notifications
                for output_file in output_files:
                    self.publish_output_file_notification(job.job_id, output_file)
                
                logger.info(f"Job completed - output: {', '.join(output_files)}")
            else:
                # Send job failed status
                self.publish_job_status(job.job_id, "failed", error_message=error_message)
                logger.error(f"Job {job.job_id} failed: {error_message}")
                
        except Exception as e:
            error_message = str(e)
            self.publish_job_status(job.job_id, "failed", error_message=error_message)
            logger.error(f"Job {job.job_id} failed with exception: {e}")

    def execute_job(self, job: JobSubmission):
        """Execute job using template rendering"""
        try:
            # Load and render template
            template_path = Path(self.template_dir) / f"{job.template}_template.j2"
            if not template_path.exists():
                raise FileNotFoundError(f"Template not found: {template_path}")
            
            with open(template_path, 'r') as f:
                template = Template(f.read())
            
            # Extract timestamp from job data
            timestamp = job.job_id.split('_')[-1]
            
            # Prepare template variables
            template_vars = {
                'job_id': job.job_id,
                'cwc_file': job.cwc_file,
                'clavrx_file': job.clavrx_file,
                'timestamp': timestamp
            }
            
            # Render script
            script_content = template.render(**template_vars)
            
            # Write script to temp file
            script_path = Path(self.temp_dir) / f"{job.job_id}.sh"
            with open(script_path, 'w') as f:
                f.write(script_content)
            
            # Make script executable
            script_path.chmod(0o755)
            
            # Execute script
            logger.info(f"Executing script: {script_path}")
            result = subprocess.run(
                ['bash', str(script_path)],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                # Parse output files from script output
                output_files = self.parse_output_files(result.stdout)
                return True, output_files, None
            else:
                return False, [], f"Script failed: {result.stderr}"
                
        except subprocess.TimeoutExpired:
            return False, [], "Job execution timed out"
        except Exception as e:
            return False, [], str(e)

    def parse_output_files(self, script_output: str):
        """Parse output files from script output"""
        output_files = []
        for line in script_output.split('\n'):
            if line.startswith('OUTPUT_FILE:'):
                output_file = line.split(':', 1)[1].strip()
                output_files.append(output_file)
        
        # If no explicit output files found, look for default pattern
        if not output_files:
            # Check for files in output directory that might have been created
            output_dir = Path(self.output_dir)
            for file_path in output_dir.glob('enhanced_clavrx_*.hdf'):
                output_files.append(str(file_path))
        
        return output_files

    def publish_job_status(self, job_id: str, status: str, output_files=None, error_message=None):
        """Publish job status update"""
        timestamp_field = f"{status}_at"
        status_data = {
            "message_type": f"job_{status}",
            "job_id": job_id,
            "status": status,
            "dispatcher_id": self.service_id,
            timestamp_field: datetime.utcnow().isoformat()
        }
        
        if output_files:
            status_data["output_files"] = output_files
        if error_message:
            status_data["error_message"] = error_message
        
        job_status = JobStatus(**status_data)
        self.rabbitmq.publish("job_status", job_status.model_dump())
        
        # Update database
        self.database.insert_job_event(
            job_id=job_id,
            status=status,
            dispatcher_id=self.service_id,
            output_files=output_files
        )

    def publish_output_file_notification(self, job_id: str, output_file: str):
        """Publish output file notification"""
        notification = OutputFileNotification(
            job_id=job_id,
            output_file=output_file,
            dispatcher_id=self.service_id,
            created_at=datetime.utcnow().isoformat()
        )
        
        self.rabbitmq.publish("output_files", notification.model_dump())

    def graceful_shutdown(self, signal_num: int, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signal_num}, shutting down gracefully...")
        self.running = False

    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        self.rabbitmq.close()
        self.database.close()
        logger.info("Dispatcher service stopped")

if __name__ == "__main__":
    service = DispatcherService()
    service.start()
