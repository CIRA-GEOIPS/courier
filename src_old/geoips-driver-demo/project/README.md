# Distributed File Processing System

A microservices-based distributed file processing system using RabbitMQ and PostgreSQL, designed for processing satellite data files when matching pairs arrive.

## Architecture

The system consists of four main components running in separate Docker containers:

- **Watcher Service**: Monitors file system for new files and publishes notifications
- **Querier Service**: Aggregates files and submits jobs when complete sets arrive  
- **Dispatcher Service**: Executes jobs using templates and publishes completion status
- **Database & RabbitMQ**: Provide persistence and message queuing

## Demo Scenario: Cloud Water Content Processing

The system processes satellite data when matching CWC and CLAVRX files arrive:

1. Files with pattern `clavrx_OR_ABI-L1b-RadF-M6C01_G16_s{timestamp}.CWC.h5`
2. Files with pattern `clavrx_OR_ABI-L1b-RadF-M6C01_G16_s{timestamp}.level2.hdf`
3. When both files with matching timestamps arrive, a processing job is submitted
4. The job executes the `add_cloud_water_content` template to create enhanced output

## Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for containers

### Run the Demo

```bash
# Clone and enter project directory
cd distributed-file-processing

# Make demo script executable
chmod +x demo.sh

# Run the complete demo
./demo.sh
```

### Manual Setup

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f

# Trigger processing by creating test files
echo "Simulated CWC data" > data/input/clavrx_OR_ABI-L1b-RadF-M6C01_G16_s20233450600210.CWC.h5
echo "Simulated CLAVRX data" > data/input/clavrx_OR_ABI-L1b-RadF-M6C01_G16_s20233450600210.level2.hdf

# Check for output
ls -la data/output/

# Stop system
docker-compose down
```

## Expected Output

When the demo runs successfully, you should see:

```
watcher-service: "Detected new file: clavrx_OR_ABI-L1b-RadF-M6C01_G16_s20233450600210.CWC.h5"
querier-service: "Collected 1/2 files for timestamp 20233450600210"
watcher-service: "Detected new file: clavrx_OR_ABI-L1b-RadF-M6C01_G16_s20233450600210.level2.hdf" 
querier-service: "Collected 2/2 files - submitting job cwc_processing_20233450600210"
dispatcher-service: "Executing job cwc_processing_20233450600210"
dispatcher-service: "Job completed - output: /data/output/enhanced_clavrx_20233450600210.hdf"
```

## System Components

### RabbitMQ Queues

- `file_notifications`: Watcher → Querier
- `job_submissions`: Querier → Dispatcher  
- `job_status`: Dispatcher → Querier
- `output_files`: Dispatcher → Querier

### Database Schema

```sql
-- File detection events
CREATE TABLE file_events (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(255),
    filename VARCHAR(255),
    file_path TEXT,
    timestamp_extracted VARCHAR(50),
    watcher_id VARCHAR(100),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job processing events  
CREATE TABLE job_events (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE,
    status VARCHAR(50),
    dispatcher_id VARCHAR(100),
    failure_count INTEGER DEFAULT 0,
    cwc_file VARCHAR(255),
    clavrx_file VARCHAR(255),
    template VARCHAR(100),
    output_files TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Message Flow

1. **File Detection**: Watcher service detects files and publishes to `file_notifications`
2. **File Aggregation**: Querier service collects files by timestamp
3. **Job Submission**: When complete sets arrive, querier publishes to `job_submissions`
4. **Job Execution**: Dispatcher service processes jobs and publishes to `job_status`
5. **Output Notification**: Completed jobs publish output files to `output_files`

## Management Interfaces

### RabbitMQ Management UI
- URL: http://localhost:15672
- Username: `admin`
- Password: `admin`

### PostgreSQL Database
```bash
# Connect to database
docker-compose exec postgres psql -U admin -d geoips_system

# View file events
SELECT * FROM file_events ORDER BY detected_at DESC;

# View job events  
SELECT * FROM job_events ORDER BY created_at DESC;
```

## Scaling

Each service can be scaled independently:

```bash
# Scale dispatcher service to 3 instances
docker-compose up -d --scale dispatcher-service=3

# Scale querier service to 2 instances
docker-compose up -d --scale querier-service=2
```

## Development

### Adding New Templates

1. Create template file in `shared/templates/`
2. Use Jinja2 syntax with variables: `job_id`, `cwc_file`, `clavrx_file`, `timestamp`
3. Output files should be written to `/data/output/`
4. Include `OUTPUT_FILE:/path/to/output` in script output

### Adding New File Patterns

1. Update regex patterns in `services/watcher/main.py`
2. Modify querier logic in `services/querier/main.py`
3. Add corresponding templates

### Monitoring and Debugging

```bash
# View all service logs
docker-compose logs

# View specific service logs
docker-compose logs -f watcher-service
docker-compose logs -f querier-service  
docker-compose logs -f dispatcher-service

# Check RabbitMQ queue status
curl -u admin:admin http://localhost:15672/api/queues

# Check database status
docker-compose exec postgres psql -U admin -d geoips_system -c "\dt"
```

## Troubleshooting

### Services Not Starting
- Check Docker daemon is running
- Ensure ports 5432, 5672, 15672 are available
- Check `docker-compose logs` for error messages

### Files Not Being Processed
- Verify file naming matches expected patterns
- Check watcher service logs for file detection
- Ensure files are placed in `data/input/` directory

### Jobs Not Executing
- Check dispatcher service logs for execution errors
- Verify template files exist in `shared/templates/`
- Check RabbitMQ connectivity

### Database Connection Issues
- Verify PostgreSQL container is healthy: `docker-compose ps`
- Check database logs: `docker-compose logs postgres`
- Test connection: `docker-compose exec postgres pg_isready`

## Architecture Benefits

- **Scalability**: Each service can be scaled independently
- **Reliability**: Message queues provide durability and retry logic
- **Monitoring**: Centralized logging and database tracking
- **Flexibility**: Template-based job execution supports various processing workflows
- **Isolation**: Each service runs in its own container with dedicated resources

This system demonstrates true microservices architecture with proper separation of concerns, message-based communication, and independent deployability.
