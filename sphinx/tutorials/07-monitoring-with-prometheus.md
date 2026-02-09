# Tutorial 7: Monitoring with Prometheus and Grafana

**Level:** Intermediate | **Time:** 40 minutes

Set up comprehensive monitoring for GeoIPS Driver services using
Prometheus for metrics collection and Grafana for visualization. Learn
to create dashboards that provide real-time insight into your satellite
data processing pipeline.

## Learning Objectives

By the end of this tutorial, you will:

-   Set up Prometheus to scrape GeoIPS Driver metrics
-   Configure Grafana for visualization
-   Create custom dashboards for monitoring
-   Set up alerting for critical conditions
-   Monitor service health and performance
-   Troubleshoot using metrics

## Prerequisites

-   Completed `` `01-simple-file-watcher ``<span class="title-ref">
    through
    :doc:</span><span class="title-ref">05-geoips-workflow-dispatcher</span>\`
-   Docker and Docker Compose installed
-   Understanding of time-series metrics
-   Familiarity with basic monitoring concepts

## Understanding the Metrics Stack

GeoIPS Driver exposes metrics on `http://localhost:8000/metrics` in
Prometheus format.

**Metric types:**

-   **Counter**: Cumulative count (files processed, errors)
-   **Gauge**: Current value (active jobs, health status)
-   **Histogram**: Distribution of values (processing duration)

**Built-in metrics:**

-   Service health and uptime
-   Plugin state and restarts
-   File processing rates
-   Job building and execution
-   RabbitMQ connection status

## Step 1: Set Up Monitoring Stack

Create `tutorial07-monitoring/docker-compose.yml`:

    version: '3.8'

    services:
      prometheus:
        image: prom/prometheus:latest
        container_name: prometheus
        ports:
          - "9090:9090"
        volumes:
          - ./prometheus.yml:/etc/prometheus/prometheus.yml
          - prometheus_data:/prometheus
        command:
          - '--config.file=/etc/prometheus/prometheus.yml'
          - '--storage.tsdb.path=/prometheus'
          - '--web.console.libraries=/usr/share/prometheus/console_libraries'
          - '--web.console.templates=/usr/share/prometheus/consoles'
        networks:
          - monitoring
        restart: unless-stopped

      grafana:
        image: grafana/grafana:latest
        container_name: grafana
        ports:
          - "3000:3000"
        environment:
          - GF_SECURITY_ADMIN_PASSWORD=admin
          - GF_USERS_ALLOW_SIGN_UP=false
        volumes:
          - grafana_data:/var/lib/grafana
          - ./grafana/provisioning:/etc/grafana/provisioning
        networks:
          - monitoring
        restart: unless-stopped
        depends_on:
          - prometheus

      # Optional: AlertManager for alerts
      alertmanager:
        image: prom/alertmanager:latest
        container_name: alertmanager
        ports:
          - "9093:9093"
        volumes:
          - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml
        command:
          - '--config.file=/etc/alertmanager/alertmanager.yml'
        networks:
          - monitoring
        restart: unless-stopped

    networks:
      monitoring:
        driver: bridge

    volumes:
      prometheus_data:
      grafana_data:

## Step 2: Configure Prometheus

Create `tutorial07-monitoring/prometheus.yml`:

    global:
      scrape_interval: 15s      # Scrape targets every 15 seconds
      evaluation_interval: 15s  # Evaluate rules every 15 seconds
      external_labels:
        monitor: 'geoips-driver'

    Alerting configuration
    ======================
    alerting:
      alertmanagers:
        - static_configs:
            - targets: ['alertmanager:9093']

    Load alerting rules
    ===================
    rule_files:
      - 'alerts.yml'

    Scrape configurations
    =====================
    scrape_configs:
      # GeoIPS Driver service
      - job_name: 'geoips-driver'
        static_configs:
          - targets: ['host.docker.internal:8000']  # macOS/Windows
            labels:
              service: 'goes18-processor'
              environment: 'production'

        # For Linux, use your host IP instead:
        # - targets: ['192.168.1.100:8000']

      # Prometheus self-monitoring
      - job_name: 'prometheus'
        static_configs:
          - targets: ['localhost:9090']

      # Add more services as needed
      - job_name: 'geoips-driver-dev'
        static_configs:
          - targets: ['host.docker.internal:8001']
            labels:
              service: 'goes18-processor-dev'
              environment: 'development'

**Key configuration:**

-   `scrape_interval`: How often to collect metrics (15s default)
-   `targets`: Where to scrape from (GeoIPS Driver services)
-   `labels`: Additional metadata for organizing metrics

## Step 3: Create Alert Rules

Create `tutorial07-monitoring/alerts.yml`:

    groups:
      - name: geoips_driver_alerts
        interval: 30s
        rules:
          # Service health alerts
          - alert: ServiceDown
            expr: service_health == 0
            for: 2m
            labels:
              severity: critical
            annotations:
              summary: "GeoIPS Driver service is down"
              description: "Service {{ $labels.service }} has been unhealthy for 2 minutes"

          # File processing alerts
          - alert: NoFilesProcessed
            expr: rate(files_processed_total[5m]) == 0
            for: 10m
            labels:
              severity: warning
            annotations:
              summary: "No files processed in 10 minutes"
              description: "Service {{ $labels.service }} hasn't processed any files"

          # Job failure rate
          - alert: HighJobFailureRate
            expr: |
              rate(dispatcher_jobs_processed_total{status="failure"}[5m])
              /
              rate(dispatcher_jobs_processed_total[5m])
              > 0.1
            for: 5m
            labels:
              severity: warning
            annotations:
              summary: "High job failure rate detected"
              description: "More than 10% of jobs are failing"

          # Plugin restart alerts
          - alert: PluginRestarting
            expr: rate(plugin_restarts_total[5m]) > 0
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: "Plugin restarting frequently"
              description: "Plugin {{ $labels.plugin_name }} is restarting"

          # Queue backlog
          - alert: JobQueueBacklog
            expr: dispatcher_active_jobs > 50
            for: 5m
            labels:
              severity: warning
            annotations:
              summary: "Large number of active jobs"
              description: "{{ $value }} jobs are currently active"

          # Processing time alerts
          - alert: SlowProcessing
            expr: |
              histogram_quantile(0.95,
                rate(dispatcher_job_execution_duration_seconds_bucket[5m])
              ) > 300
            for: 10m
            labels:
              severity: warning
            annotations:
              summary: "Slow job processing detected"
              description: "95th percentile processing time is {{ $value }}s"

## Step 4: Configure AlertManager

Create `tutorial07-monitoring/alertmanager.yml`:

    global:
      resolve_timeout: 5m
      smtp_smarthost: 'smtp.example.com:587'
      smtp_from: 'alerts@example.com'
      smtp_auth_username: 'alerts@example.com'
      smtp_auth_password: 'password'

    route:
      group_by: ['alertname', 'service']
      group_wait: 10s
      group_interval: 10s
      repeat_interval: 12h
      receiver: 'default'

      routes:
        # Critical alerts go to PagerDuty
        - match:
            severity: critical
          receiver: 'pagerduty'

        # Warnings go to email
        - match:
            severity: warning
          receiver: 'email'

    receivers:
      - name: 'default'
        webhook_configs:
          - url: 'http://webhook-receiver:8080/alerts'

      - name: 'email'
        email_configs:
          - to: 'ops-team@example.com'
            headers:
              Subject: 'GeoIPS Driver Alert: {{ .GroupLabels.alertname }}'

      - name: 'pagerduty'
        pagerduty_configs:
          - service_key: 'your-pagerduty-service-key'

      - name: 'slack'
        slack_configs:
          - api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
            channel: '#geoips-alerts'
            title: 'GeoIPS Driver Alert'
            text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'

## Step 5: Set Up Grafana Dashboards

Create the provisioning directory:

    mkdir -p tutorial07-monitoring/grafana/provisioning/{datasources,dashboards}

Configure Prometheus datasource:

`grafana/provisioning/datasources/prometheus.yml`:

    apiVersion: 1

    datasources:
      - name: Prometheus
        type: prometheus
        access: proxy
        url: http://prometheus:9090
        isDefault: true
        editable: true

Configure dashboard provisioning:

`grafana/provisioning/dashboards/default.yml`:

    apiVersion: 1

    providers:
      - name: 'GeoIPS Driver'
        orgId: 1
        folder: ''
        type: file
        disableDeletion: false
        updateIntervalSeconds: 10
        allowUiUpdates: true
        options:
          path: /etc/grafana/provisioning/dashboards

## Step 6: Create GeoIPS Driver Dashboard

Create `grafana/provisioning/dashboards/geoips-driver-overview.json`:

    {
      "dashboard": {
        "title": "GeoIPS Driver - Overview",
        "tags": ["geoips", "satellite", "monitoring"],
        "timezone": "browser",
        "panels": [
          {
            "title": "Service Health",
            "type": "stat",
            "targets": [{
              "expr": "service_health",
              "legendFormat": "{{ service }}"
            }],
            "fieldConfig": {
              "defaults": {
                "mappings": [
                  {"value": 1, "text": "Healthy"},
                  {"value": 0, "text": "Unhealthy"}
                ],
                "thresholds": {
                  "steps": [
                    {"value": 0, "color": "red"},
                    {"value": 1, "color": "green"}
                  ]
                }
              }
            },
            "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0}
          },
          {
            "title": "Service Uptime",
            "type": "stat",
            "targets": [{
              "expr": "service_uptime_seconds"
            }],
            "fieldConfig": {
              "defaults": {
                "unit": "s"
              }
            },
            "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0}
          },
          {
            "title": "Files Processed (Total)",
            "type": "stat",
            "targets": [{
              "expr": "sum(files_processed_total)"
            }],
            "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0}
          },
          {
            "title": "Active Jobs",
            "type": "stat",
            "targets": [{
              "expr": "sum(dispatcher_active_jobs)"
            }],
            "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0}
          },
          {
            "title": "File Processing Rate",
            "type": "graph",
            "targets": [{
              "expr": "rate(files_processed_total[5m])",
              "legendFormat": "{{ status }}"
            }],
            "yaxes": [
              {"label": "files/sec", "show": true},
              {"show": false}
            ],
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4}
          },
          {
            "title": "Job Execution Duration (p95)",
            "type": "graph",
            "targets": [{
              "expr": "histogram_quantile(0.95, rate(dispatcher_job_execution_duration_seconds_bucket[5m]))",
              "legendFormat": "{{ dispatcher_name }}"
            }],
            "yaxes": [
              {"label": "seconds", "show": true},
              {"show": false}
            ],
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4}
          },
          {
            "title": "Job Success vs Failure Rate",
            "type": "graph",
            "targets": [
              {
                "expr": "rate(dispatcher_jobs_processed_total{status='success'}[5m])",
                "legendFormat": "Success"
              },
              {
                "expr": "rate(dispatcher_jobs_processed_total{status='failure'}[5m])",
                "legendFormat": "Failure"
              }
            ],
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": 12}
          },
          {
            "title": "Plugin Status",
            "type": "table",
            "targets": [{
              "expr": "plugin_state",
              "format": "table",
              "instant": true
            }],
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": 12}
          }
        ],
        "refresh": "10s",
        "time": {"from": "now-1h", "to": "now"}
      }
    }

## Step 7: Start the Monitoring Stack

    cd tutorial07-monitoring

    Start Prometheus and Grafana
    ============================
    docker-compose up -d

    Check status
    ============
    docker-compose ps

    View logs
    =========
    docker-compose logs -f

**Access services:**

-   Prometheus: <http://localhost:9090>
-   Grafana: <http://localhost:3000> (admin/admin)
-   AlertManager: <http://localhost:9093>

## Step 8: Configure GeoIPS Driver Service

Run a GeoIPS Driver service to generate metrics:

`service_config.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: monitored-service
    description: Service with monitoring enabled.

    spec:
      service_namespace: monitoring_demo
      heartbeat_interval: 15  # Match Prometheus scrape interval

      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_test

      run:
        - monitor:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/incoming
              metadata-tools: [goes18_abi]

        - build:
            kind: job_builder
            name: DummyJobBuilder
            config: null

        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                echo "Processing {file}"
                sleep 2  # Simulate processing time

Start the service:

    geoips-driver run service_config.yaml

## Step 9: Explore Prometheus

Open <http://localhost:9090>

**Query examples:**

1.  **Service health:**

    \`\`\`text

    service\_health

<!-- -->

    2. **File processing rate (last 5 minutes):**

       ```text

       rate(files_processed_total[5m])

1.  **95th percentile job duration:**

    \`\`\`text

    histogram\_quantile(0.95,
    rate(dispatcher\_job\_execution\_duration\_seconds\_bucket\[5m\]))

<!-- -->

    4. **Job failure rate:**

       ```text

       rate(dispatcher_jobs_processed_total{status="failure"}[5m])
       /
       rate(dispatcher_jobs_processed_total[5m])

## Step 10: Create Grafana Dashboards

Open <http://localhost:3000> (admin/admin)

**Create a new dashboard:**

1.  Click "+" → "Dashboard"
2.  Add Panel
3.  Select "Time series" visualization
4.  Enter query: `rate(files_processed_total[5m])`
5.  Configure legend, axes, thresholds
6.  Save panel

**Import community dashboards:**

1.  Go to Dashboards → Import
2.  Enter dashboard ID: 1860 (Node Exporter Full)
3.  Select Prometheus datasource
4.  Import

**Key panels to create:**

-   **Service Overview**: Health, uptime, file counts
-   **Processing Performance**: Rates, durations, throughput
-   **Error Tracking**: Failure rates, plugin restarts
-   **Queue Metrics**: Active jobs, backlog
-   **Resource Usage**: If running with node\_exporter

## Step 11: Set Up Alerts in Grafana

Create alert rules in Grafana:

1.  Go to Alerting → Alert rules
2.  Create alert rule
3.  Configure query and condition:

<!-- -->

    Alert when service is down
    ==========================
    service_health < 1

    Alert when no files processed in 10 minutes
    ===========================================
    rate(files_processed_total[10m]) == 0

    Alert on high failure rate
    ==========================
    rate(dispatcher_jobs_processed_total{status="failure"}[5m]) > 0.1

    [``

    4. Configure notification channel (email, Slack, PagerDuty)

    5. Save alert

    Step 12: Advanced Monitoring
    ----------------------------

    **Add custom metrics:**

    Create a custom dispatcher with metrics:

    ```python

    from prometheus_client import Counter, Histogram

    class CustomDispatcher(Dispatcher):
        def __init__(self, service, config):
            super().__init__(service, config)

            # Custom metrics
            self.custom_metric = Counter(
                'custom_processing_events_total',
                'Custom processing events',
                ['event_type']
            )

            self.processing_time = Histogram(
                'custom_processing_seconds',
                'Custom processing time',
                buckets=(1, 5, 10, 30, 60, 120)
            )

        def get_execution_log(self, job):
            with self.processing_time.time():
                # Processing logic
                self.custom_metric.labels(event_type='started').inc()
                # ... process ...
                self.custom_metric.labels(event_type='completed').inc()

**Monitor RabbitMQ:**

Add RabbitMQ exporter to docker-compose.yml:

    rabbitmq-exporter:
      image: kbudde/rabbitmq-exporter
      ports:
        - "9419:9419"
      environment:
        - RABBIT_URL=http://rabbitmq:15672

Add to Prometheus config:

    - job_name: 'rabbitmq'
      static_configs:
        - targets: ['rabbitmq-exporter:9419']

## Testing and Validation

**Generate test load:**

    Create files to process
    =======================
    for i in {1..100}; do
        touch data/incoming/test_${i}.nc
        sleep 1
    done

**Watch metrics update:**

    Query Prometheus
    ================
    curl 'http://localhost:9090/api/v1/query?query=files_processed_total'

    Check Grafana dashboard
    =======================
    Should see file processing rate increase
    ========================================

**Test alerting:**

    Stop the service to trigger alerts
    ==================================
    Kill the GeoIPS Driver process
    ==============================

    Wait 2-5 minutes
    ================
    Check AlertManager: http://localhost:9093
    =========================================
    Should see "ServiceDown" alert
    ==============================

## Best Practices

1.  **Scrape interval**: Balance freshness vs load (15-30s typical)
2.  **Retention**: Configure Prometheus retention (default 15 days)
3.  **High availability**: Run multiple Prometheus instances
4.  **Dashboard organization**: Group by service, environment
5.  **Alert fatigue**: Set appropriate thresholds and delays
6.  **Documentation**: Document what each metric means

## Troubleshooting

**Prometheus not scraping:**

-   Check target status: <http://localhost:9090/targets>
-   Verify service is exposing metrics:
    `curl http://localhost:8000/metrics`
-   Check network connectivity from container

**Metrics not appearing:**

-   Verify metric names:
    `curl http://localhost:8000/metrics | grep metric_name`
-   Check Prometheus logs: `docker-compose logs prometheus`
-   Validate PromQL query syntax

**Grafana dashboard issues:**

-   Verify datasource connection: Configuration → Data Sources
-   Check query in Explore tab
-   Review Grafana logs: `docker-compose logs grafana`

## What You Learned

✅ Prometheus setup and configuration ✅ Creating alert rules ✅ Grafana
dashboard creation ✅ Monitoring GeoIPS Driver services ✅ Custom
metrics implementation ✅ Alerting and notification setup ✅
Troubleshooting with metrics

## Next Steps

-   `` `08-production-deployment ``\` - Deploy to Kubernetes with
    monitoring
-   :doc:`../user-guide/monitoring` - Complete monitoring guide
-   :doc:`../reference/metrics-reference` - All available metrics

## Challenge Exercises

1.  **Create custom dashboard** - Build sector-specific monitoring
2.  **Add Loki integration** - Centralized log aggregation
3.  **Set up tracing** - Add Jaeger for distributed tracing
4.  **Create SLO dashboard** - Service Level Objectives tracking

## Complete Code

\`tutorial07-monitoring/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/07-monitoring>)
