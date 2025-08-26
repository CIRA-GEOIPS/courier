import psycopg2
import psycopg2.pool
import logging
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self, database_url: str, min_connections: int = 1, max_connections: int = 10):
        self.database_url = database_url
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            min_connections, max_connections, database_url
        )
        logger.info("Database connection pool created")

    @contextmanager
    def get_connection(self):
        """Get database connection from pool"""
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def insert_file_event(self, filename: str, file_path: str, timestamp_extracted: str, watcher_id: str, batch_id: str = None):
        """Insert file detection event"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO file_events (batch_id, filename, file_path, timestamp_extracted, watcher_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (batch_id, filename, file_path, timestamp_extracted, watcher_id)
            )
            conn.commit()
            logger.info(f"Inserted file event: {filename}")

    def insert_job_event(self, job_id: str, status: str, dispatcher_id: str = None, 
                        cwc_file: str = None, clavrx_file: str = None, template: str = None,
                        output_files: List[str] = None, failure_count: int = 0):
        """Insert or update job event"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO job_events (job_id, status, dispatcher_id, cwc_file, clavrx_file, template, output_files, failure_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    dispatcher_id = EXCLUDED.dispatcher_id,
                    output_files = EXCLUDED.output_files,
                    failure_count = EXCLUDED.failure_count,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (job_id, status, dispatcher_id, cwc_file, clavrx_file, template, output_files, failure_count)
            )
            conn.commit()
            logger.info(f"Inserted/updated job event: {job_id} - {status}")

    def get_files_by_timestamp(self, timestamp: str) -> List[Dict[str, Any]]:
        """Get all files for a given timestamp"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT filename, file_path, timestamp_extracted, detected_at
                FROM file_events
                WHERE timestamp_extracted = %s
                ORDER BY detected_at
                """,
                (timestamp,)
            )
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def close(self):
        """Close all database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connections closed")
