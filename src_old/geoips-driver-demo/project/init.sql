-- Database initialization script for PostgreSQL

-- File events table
CREATE TABLE IF NOT EXISTS file_events (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(255),
    filename VARCHAR(255),
    file_path TEXT,
    timestamp_extracted VARCHAR(50),
    watcher_id VARCHAR(100),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job events table
CREATE TABLE IF NOT EXISTS job_events (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE,
    status VARCHAR(50), -- 'submitted', 'accepted', 'completed', 'failed'
    dispatcher_id VARCHAR(100),
    failure_count INTEGER DEFAULT 0,
    cwc_file VARCHAR(255),
    clavrx_file VARCHAR(255),
    template VARCHAR(100),
    output_files TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_file_events_timestamp ON file_events(timestamp_extracted);
CREATE INDEX IF NOT EXISTS idx_file_events_filename ON file_events(filename);
CREATE INDEX IF NOT EXISTS idx_job_events_status ON job_events(status);
CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;
