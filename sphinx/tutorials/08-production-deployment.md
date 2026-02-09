# Tutorial 8: Production Deployment to Kubernetes

**Level:** Advanced | **Time:** 45 minutes

Deploy GeoIPS Driver to a production Kubernetes cluster with high
availability, automatic scaling, and comprehensive monitoring. Learn
cloud-native deployment patterns for satellite data processing services.

## Learning Objectives

By the end of this tutorial, you will:

-   Create Kubernetes manifests for GeoIPS Driver
-   Deploy with high availability and scaling
-   Configure persistent storage for data
-   Set up secrets management
-   Implement health checks and readiness probes
-   Deploy monitoring stack in Kubernetes
-   Configure ingress and load balancing

## Prerequisites

-   Completed `` `01-simple-file-watcher ``<span class="title-ref">
    through
    :doc:</span><span class="title-ref">07-monitoring-with-prometheus</span>\`
-   Kubernetes cluster (minikube, kind, or cloud provider)
-   kubectl installed and configured
-   Understanding of Kubernetes concepts (Pods, Deployments, Services)
-   Docker registry access for custom images

## Understanding Kubernetes Deployment

**Key components:**

-   **Deployment**: Manages GeoIPS Driver pods
-   **ConfigMap**: Stores service configuration
-   **Secret**: Stores sensitive credentials
-   **PersistentVolume**: Storage for data and products
-   **Service**: Exposes metrics endpoint
-   **ServiceMonitor**: Prometheus scraping config
-   **Ingress**: External access (optional)

**Architecture:**

    ┌──────────────────────────────────────────────────────────┐
    │                    Kubernetes Cluster                    │
    │                                                          │
    │  ┌────────────────┐      ┌────────────────┐             │
    │  │  GeoIPS Driver │◄─────┤   ConfigMap    │             │
    │  │   Deployment   │      └────────────────┘             │
    │  │                │                                      │
    │  │ ┌────────────┐ │      ┌────────────────┐             │
    │  │ │    Pod 1   │ │◄─────┤     Secret     │             │
    │  │ └────────────┘ │      └────────────────┘             │
    │  │ ┌────────────┐ │                                      │
    │  │ │    Pod 2   │ │      ┌────────────────┐             │
    │  │ └────────────┘ │◄─────┤ PersistentVol  │             │
    │  └────────────────┘      └────────────────┘             │
    │         │                                                │
    │         ▼                                                │
    │  ┌────────────────┐      ┌────────────────┐             │
    │  │   Service      │─────▶│  Prometheus    │             │
    │  │  (Metrics)     │      │   Operator     │             │
    │  └────────────────┘      └────────────────┘             │
    └──────────────────────────────────────────────────────────┘

## Step 1: Create Namespace

`kubernetes/namespace.yaml`:

    apiVersion: v1
    kind: Namespace
    metadata:
      name: geoips-driver
      labels:
        name: geoips-driver
        environment: production

Apply:

    kubectl apply -f kubernetes/namespace.yaml

## Step 2: Create ConfigMap for Service Configuration

`kubernetes/configmap.yaml`:

    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: geoips-driver-config
      namespace: geoips-driver
    data:
      service-config.yaml: |
        apiVersion: geoips_driver/v1
        kind: Service
        name: goes18-processor
        description: Production GOES-18 processing service.

        spec:
          service_namespace: production
          heartbeat_interval: 30

          rabbitmq:
            host: rabbitmq.geoips-driver.svc.cluster.local
            port: 5672
            username: ${RABBITMQ_USER}
            password: ${RABBITMQ_PASSWORD}

          run:
            - monitor:
                kind: data_monitor
                name: file_system_poller_watchdog
                config:
                  path: /data/incoming
                  metadata-tools: [goes18_abi]

            - build:
                kind: job_builder
                name: ChannelGroupBuilder
                config:
                  timeout_seconds: 600

            - process:
                kind: dispatcher
                name: serial_bash
                config:
                  bash_script: |
                    #!/bin/bash
                    set -e
                    geoips run single_source {file} \
                      --reader_name abi_netcdf \
                      --product_name True-Color \
                      --output_formatter imagery_clean \
                      --filename_formatter geoips_fname \
                      --output_dir /data/products

Apply:

    kubectl apply -f kubernetes/configmap.yaml

## Step 3: Create Secrets

`kubernetes/secret.yaml`:

    apiVersion: v1
    kind: Secret
    metadata:
      name: geoips-driver-secrets
      namespace: geoips-driver
    type: Opaque
    stringData:
      rabbitmq-user: "geoips_processor"
      rabbitmq-password: "CHANGE_ME_IN_PRODUCTION"
      loki-url: "http://loki.monitoring.svc.cluster.local:3100/loki/api/v1/push"

**For production, use SealedSecrets or external secret management:**

    Using kubectl (not recommended for production)
    ==============================================
    kubectl create secret generic geoips-driver-secrets \
      --from-literal=rabbitmq-user=geoips_processor \
      --from-literal=rabbitmq-password=$(openssl rand -base64 32) \
      -n geoips-driver

    Or use SealedSecrets
    ====================
    kubeseal --format=yaml < secret.yaml > sealed-secret.yaml

## Step 4: Create PersistentVolumeClaims

`kubernetes/pvc.yaml`:

    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: geoips-incoming-data
      namespace: geoips-driver
    spec:
      accessModes:
        - ReadWriteMany  # Multiple pods can read/write
      resources:
        requests:
          storage: 500Gi
      storageClassName: nfs-storage  # Use appropriate storage class
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: geoips-products
      namespace: geoips-driver
    spec:
      accessModes:
        - ReadWriteMany
      resources:
        requests:
          storage: 2Ti
      storageClassName: nfs-storage

**Note:** Adjust storage class based on your cluster (nfs, ceph,
aws-ebs, etc.)

Apply:

    kubectl apply -f kubernetes/pvc.yaml

## Step 5: Create Deployment

`kubernetes/deployment.yaml`:

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: geoips-driver
      namespace: geoips-driver
      labels:
        app: geoips-driver
        version: v1
    spec:
      replicas: 2  # High availability
      selector:
        matchLabels:
          app: geoips-driver
      template:
        metadata:
          labels:
            app: geoips-driver
            version: v1
          annotations:
            prometheus.io/scrape: "true"
            prometheus.io/port: "8000"
            prometheus.io/path: "/metrics"
        spec:
          serviceAccountName: geoips-driver

          # Anti-affinity: spread pods across nodes
          affinity:
            podAntiAffinity:
              preferredDuringSchedulingIgnoredDuringExecution:
                - weight: 100
                  podAffinityTerm:
                    labelSelector:
                      matchLabels:
                        app: geoips-driver
                    topologyKey: kubernetes.io/hostname

          initContainers:
            # Ensure directories exist
            - name: init-directories
              image: busybox:latest
              command:
                - sh
                - -c
                - |
                  mkdir -p /data/incoming /data/products
                  chmod 777 /data/incoming /data/products
              volumeMounts:
                - name: incoming-data
                  mountPath: /data/incoming
                - name: products
                  mountPath: /data/products

          containers:
            - name: geoips-driver
              image: ghcr.io/your-org/geoips-driver:latest
              imagePullPolicy: Always

              env:
                # Service configuration
                - name: SERVICE_ID
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
                - name: SERVICE_NAMESPACE
                  value: "production"

                # RabbitMQ credentials from secret
                - name: RABBITMQ_USER
                  valueFrom:
                    secretKeyRef:
                      name: geoips-driver-secrets
                      key: rabbitmq-user
                - name: RABBITMQ_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: geoips-driver-secrets
                      key: rabbitmq-password

                # Loki logging
                - name: LOKI_URL
                  valueFrom:
                    secretKeyRef:
                      name: geoips-driver-secrets
                      key: loki-url
                - name: LOKI_ENABLED
                  value: "true"

                # Logging
                - name: LOG_LEVEL
                  value: "INFO"
                - name: PRODUCTION
                  value: "true"

                # Prometheus
                - name: PROMETHEUS_PORT
                  value: "8000"

              command:
                - geoips-driver
                - run
                - /config/service-config.yaml

              ports:
                - name: metrics
                  containerPort: 8000
                  protocol: TCP

              volumeMounts:
                - name: config
                  mountPath: /config
                - name: incoming-data
                  mountPath: /data/incoming
                - name: products
                  mountPath: /data/products

              # Resource limits
              resources:
                requests:
                  memory: "2Gi"
                  cpu: "1000m"
                limits:
                  memory: "4Gi"
                  cpu: "2000m"

              # Health checks
              livenessProbe:
                httpGet:
                  path: /metrics
                  port: metrics
                initialDelaySeconds: 30
                periodSeconds: 10
                timeoutSeconds: 5
                failureThreshold: 3

              readinessProbe:
                httpGet:
                  path: /metrics
                  port: metrics
                initialDelaySeconds: 10
                periodSeconds: 5
                timeoutSeconds: 3
                failureThreshold: 2

          volumes:
            - name: config
              configMap:
                name: geoips-driver-config
            - name: incoming-data
              persistentVolumeClaim:
                claimName: geoips-incoming-data
            - name: products
              persistentVolumeClaim:
                claimName: geoips-products

Apply:

    kubectl apply -f kubernetes/deployment.yaml

## Step 6: Create Service for Metrics

`kubernetes/service.yaml`:

    apiVersion: v1
    kind: Service
    metadata:
      name: geoips-driver-metrics
      namespace: geoips-driver
      labels:
        app: geoips-driver
    spec:
      type: ClusterIP
      selector:
        app: geoips-driver
      ports:
        - name: metrics
          port: 8000
          targetPort: 8000
          protocol: TCP

Apply:

    kubectl apply -f kubernetes/service.yaml

## Step 7: Deploy RabbitMQ

`kubernetes/rabbitmq.yaml`:

    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      name: rabbitmq
      namespace: geoips-driver
    spec:
      serviceName: rabbitmq
      replicas: 1
      selector:
        matchLabels:
          app: rabbitmq
      template:
        metadata:
          labels:
            app: rabbitmq
        spec:
          containers:
            - name: rabbitmq
              image: rabbitmq:3-management
              ports:
                - name: amqp
                  containerPort: 5672
                - name: management
                  containerPort: 15672
              env:
                - name: RABBITMQ_DEFAULT_USER
                  valueFrom:
                    secretKeyRef:
                      name: geoips-driver-secrets
                      key: rabbitmq-user
                - name: RABBITMQ_DEFAULT_PASS
                  valueFrom:
                    secretKeyRef:
                      name: geoips-driver-secrets
                      key: rabbitmq-password
              volumeMounts:
                - name: rabbitmq-data
                  mountPath: /var/lib/rabbitmq
      volumeClaimTemplates:
        - metadata:
            name: rabbitmq-data
          spec:
            accessModes: ["ReadWriteOnce"]
            resources:
              requests:
                storage: 50Gi
    ---
    apiVersion: v1
    kind: Service
    metadata:
      name: rabbitmq
      namespace: geoips-driver
    spec:
      selector:
        app: rabbitmq
      ports:
        - name: amqp
          port: 5672
        - name: management
          port: 15672

Apply:

    kubectl apply -f kubernetes/rabbitmq.yaml

## Step 8: Configure Prometheus Monitoring

`kubernetes/servicemonitor.yaml`:

    apiVersion: monitoring.coreos.com/v1
    kind: ServiceMonitor
    metadata:
      name: geoips-driver
      namespace: geoips-driver
      labels:
        app: geoips-driver
    spec:
      selector:
        matchLabels:
          app: geoips-driver
      endpoints:
        - port: metrics
          interval: 15s
          path: /metrics

**Requires Prometheus Operator to be installed.**

Apply:

    kubectl apply -f kubernetes/servicemonitor.yaml

## Step 9: Create ServiceAccount and RBAC

`kubernetes/rbac.yaml`:

    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: geoips-driver
      namespace: geoips-driver
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      name: geoips-driver
      namespace: geoips-driver
    rules:
      - apiGroups: [""]
        resources: ["configmaps"]
        verbs: ["get", "list", "watch"]
      - apiGroups: [""]
        resources: ["secrets"]
        verbs: ["get", "list"]
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: RoleBinding
    metadata:
      name: geoips-driver
      namespace: geoips-driver
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: Role
      name: geoips-driver
    subjects:
      - kind: ServiceAccount
        name: geoips-driver
        namespace: geoips-driver

Apply:

    kubectl apply -f kubernetes/rbac.yaml

## Step 10: Deploy with Helm (Optional but Recommended)

Create a Helm chart for easier management:

    mkdir -p helm/geoips-driver
    cd helm/geoips-driver

`Chart.yaml`:

    apiVersion: v2
    name: geoips-driver
    description: GeoIPS Driver for near real-time satellite processing
    version: 1.0.0
    appVersion: "1.0.0"
    keywords:
      - satellite
      - geoips
      - processing

`values.yaml`:

    replicaCount: 2

    image:
      repository: ghcr.io/your-org/geoips-driver
      tag: latest
      pullPolicy: Always

    service:
      type: ClusterIP
      port: 8000

    resources:
      requests:
        memory: 2Gi
        cpu: 1000m
      limits:
        memory: 4Gi
        cpu: 2000m

    persistence:
      incoming:
        enabled: true
        size: 500Gi
        storageClass: nfs-storage
      products:
        enabled: true
        size: 2Ti
        storageClass: nfs-storage

    rabbitmq:
      enabled: true
      auth:
        username: geoips_processor
        # Use existing secret in production
        existingPasswordSecret: geoips-driver-secrets

    monitoring:
      enabled: true
      prometheus:
        enabled: true

Install with Helm:

    helm install geoips-driver ./helm/geoips-driver \
      --namespace geoips-driver \
      --create-namespace

## Step 11: Configure Horizontal Pod Autoscaling

`kubernetes/hpa.yaml`:

    apiVersion: autoscaling/v2
    kind: HorizontalPodAutoscaler
    metadata:
      name: geoips-driver
      namespace: geoips-driver
    spec:
      scaleTargetRef:
        apiVersion: apps/v1
        kind: Deployment
        name: geoips-driver
      minReplicas: 2
      maxReplicas: 10
      metrics:
        - type: Resource
          resource:
            name: cpu
            target:
              type: Utilization
              averageUtilization: 70
        - type: Resource
          resource:
            name: memory
            target:
              type: Utilization
              averageUtilization: 80
        - type: Pods
          pods:
            metric:
              name: dispatcher_active_jobs
            target:
              type: AverageValue
              averageValue: "20"

Apply:

    kubectl apply -f kubernetes/hpa.yaml

## Step 12: Verify Deployment

    Check all resources
    ===================
    kubectl get all -n geoips-driver

    Check pods
    ==========
    kubectl get pods -n geoips-driver

    Check logs
    ==========
    kubectl logs -f deployment/geoips-driver -n geoips-driver

    Check services
    ==============
    kubectl get svc -n geoips-driver

    Check PVCs
    ==========
    kubectl get pvc -n geoips-driver

    Port-forward for testing
    ========================
    kubectl port-forward svc/geoips-driver-metrics 8000:8000 -n geoips-driver

Access metrics:

    curl http://localhost:8000/metrics

## Step 13: Update and Rollback

**Update deployment:**

    Update image
    ============
    kubectl set image deployment/geoips-driver \
      geoips-driver=ghcr.io/your-org/geoips-driver:v1.1.0 \
      -n geoips-driver

    Check rollout status
    ====================
    kubectl rollout status deployment/geoips-driver -n geoips-driver

**Rollback if needed:**

    View history
    ============
    kubectl rollout history deployment/geoips-driver -n geoips-driver

    Rollback to previous
    ====================
    kubectl rollout undo deployment/geoips-driver -n geoips-driver

    Rollback to specific revision
    =============================
    kubectl rollout undo deployment/geoips-driver --to-revision=2 -n geoips-driver

## Production Best Practices

1.  **Use Helm for deployment management**
2.  **Implement proper secret management** (SealedSecrets, Vault)
3.  **Configure resource requests and limits**
4.  **Set up pod disruption budgets**
5.  **Use namespaces for isolation**
6.  **Implement network policies**
7.  **Configure backup and disaster recovery**
8.  **Set up log aggregation** (ELK, Loki)
9.  **Monitor with Prometheus and Grafana**
10. **Implement GitOps** (ArgoCD, Flux)

## Troubleshooting

**Pods not starting:**

    kubectl describe pod <pod-name> -n geoips-driver
    kubectl logs <pod-name> -n geoips-driver

    ```
    **PVC issues:**

    ```bash

    kubectl get pvc -n geoips-driver
    kubectl describe pvc geoips-incoming-data -n geoips-driver

**Service not accessible:**

    kubectl get svc -n geoips-driver
    kubectl get endpoints -n geoips-driver

**Check events:**

    kubectl get events -n geoips-driver --sort-by='.lastTimestamp'

## What You Learned

✅ Kubernetes deployment patterns for GeoIPS Driver ✅ ConfigMap and
Secret management ✅ Persistent storage configuration ✅ High
availability setup ✅ Horizontal pod autoscaling ✅ Monitoring
integration ✅ Production best practices

## Next Steps

-   :doc:`../user-guide/deployment` - Complete deployment guide
-   `` `09-error-handling ``\` - Handle failures in production
-   :doc:`../user-guide/monitoring` - Production monitoring

## Challenge Exercises

1.  **Implement GitOps** - Use ArgoCD for automated deployments
2.  **Add network policies** - Restrict pod-to-pod communication
3.  **Configure pod disruption budget** - Ensure availability during
    updates
4.  **Set up backup** - Automated PVC snapshots

## Complete Code

`tutorial08-kubernetes/  <https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/08-kubernetes>`\_
