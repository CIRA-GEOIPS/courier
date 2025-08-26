from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

class FileNotification(BaseModel):
    message_type: str = "file_detected"
    batch_id: Optional[str] = None
    filename: str
    file_path: str
    timestamp: Optional[str] = None
    watcher_id: str
    detected_at: str

class JobSubmission(BaseModel):
    message_type: str = "job_request"
    job_id: str
    cwc_file: str
    clavrx_file: str
    template: str
    failure_count: int = 0
    requirements: Dict[str, Any] = {"cores": 1, "memory_gb": 2}
    submitted_by: str
    submitted_at: str

class JobStatus(BaseModel):
    message_type: str  # "job_completed", "job_failed", "job_accepted"
    job_id: str
    status: str
    dispatcher_id: str
    output_files: Optional[List[str]] = None
    error_message: Optional[str] = None
    completed_at: Optional[str] = None
    failed_at: Optional[str] = None
    accepted_at: Optional[str] = None

class OutputFileNotification(BaseModel):
    message_type: str = "output_file_created"
    job_id: str
    output_file: str
    dispatcher_id: str
    created_at: str
