---
layout: default
title: "4.9 — Streaming / Event-Driven · Hybrid OSS Self-Hosted"
---

# 4.9 — Streaming / Event-Driven · Hybrid OSS Self-Hosted (On-Prem + Cloud)

**Stack:** Apache Kafka on Kubernetes · Apache Flink on Kubernetes · Apache Cassandra · MinIO
**Processing:** Streaming-first · Kappa Architecture · On-Prem Core + Cloud Burst
**Buy vs Build:** Full Build (self-hosted OSS, full infrastructure ownership)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["On-Prem Event Sources"]
        S1[Core Banking / ERP]
        S2[SCADA / IoT Sensors]
        S3[Retail POS]
        S4[Debezium CDC · on-prem DB]
    end

    subgraph BROKER["On-Prem Event Broker — Kafka · Strimzi on K8s"]
        B1[Kafka 3-broker cluster · KRaft mode\nConfluent Schema Registry · Avro]
        B2[MirrorMaker 2\nactive-passive DR replica → cloud Kafka]
    end

    subgraph PROC["Stream Processing — Apache Flink · K8s Operator"]
        P1[Enrichment\nAsync I/O Cassandra lookup]
        P2[Window Aggregation\ntumbling · sliding · session]
        P3[CEP Fraud / Anomaly Detection\nFlink CEP library]
    end

    subgraph SERVING["Serving Stores — On-Prem + Cloud Tier"]
        D1[(Cassandra · K8ssandra on K8s\nHot · <5ms · RF=3 · TTL)]
        D2[Iceberg on MinIO · on-prem\nCold · ACID · S3A compatible]
        D3[Cloud Object Storage · S3/ADLS/GCS\nLifecycle tiered from MinIO after 90d]
    end

    subgraph CATALOG["Catalog & Access Control"]
        C1[Nessie REST Catalog · on-prem K8s\nauthoritative · git-branching]
        C2[Apache Ranger · K8s\ncolumn masking · row filtering]
        C3[Trino · on-prem K8s\nSQL federation · Iceberg + Cassandra]
    end

    subgraph CONSUME["Consumers"]
        F1[On-Prem REST APIs\nCassandra reads]
        F2[Apache Superset\nTrino on-prem]
        F3[Cloud ML Training\ncloud object storage]
    end

    SRC --> BROKER
    BROKER --> PROC
    PROC --> D1 & D2
    D2 -.->|lifecycle rule| D3
    D2 -. register .-> C1
    C1 -. policies .-> C2
    C1 --> C3
    D1 --> F1
    C3 --> F2
    D3 --> F3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["On-Prem Sources"]
        A1[Core Banking / ERP]
        A2[SCADA / IoT Sensors]
        A3[Retail POS]
        A4[Debezium CDC\non-prem DB]
    end

    subgraph Broker["Kafka on K8s — Strimzi"]
        B1[Kafka Topics\non-prem cluster]
        B2[Schema Registry\nAvro enforcement]
        B3[MirrorMaker 2\ncloud DR replica]
    end

    subgraph Processing["Apache Flink — K8s Operator"]
        C1[Enrichment\nCassandra Async I/O]
        C2[Window Aggregation\ntumbling · sliding]
        C3[CEP Fraud Detection\nFlink CEP]
    end

    subgraph Serving["Serving Stores"]
        D1[(Cassandra on K8s\nHot — <5ms)]
        D2[Iceberg on MinIO\nCold — on-prem ACID]
        D3[Cloud Object Storage\naged-out cold archive]
    end

    subgraph Catalog["Catalog + Security"]
        E1[Nessie REST Catalog\non-prem authoritative]
        E2[Apache Ranger\ncolumn/row policies]
        E3[Trino on K8s\nSQL federation]
    end

    subgraph Consume
        F1[On-Prem APIs\nCassandra]
        F2[Superset\nTrino on-prem]
        F3[Cloud ML\ncloud object store]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B1
    B1 -. schema validate .-> B2
    B1 -.->|MirrorMaker 2| B3

    B1 --> C1
    C1 --> C2
    C1 --> C3
    C2 --> D1
    C3 --> D1
    C2 --> D2
    B1 -.->|Kafka Connect MinIO Sink| D2

    D2 -. lifecycle rule .-> D3
    D2 -. register .-> E1
    E1 -. policies .-> E2
    E1 --> E3

    D1 --> F1
    E3 --> F2
    D3 --> F3
```

---

## Stream Store Design

```
HOT PATH  — Apache Cassandra on Kubernetes
  Namespace: cassandra  (K8s StatefulSet via K8ssandra Operator)
  Keyspace: streaming_hot  (RF=3, local DC)
  Table: entity_state
    PK: entity_id   CK: event_type, event_time
    TTL: 86400s (1 day)
  Table: window_aggregates
    PK: (entity_id, window_type)  CK: window_start
    TTL: 604800s (7 days)

COLD PATH — Apache Iceberg on MinIO (on-prem S3-compatible)
  s3a://streaming-iceberg/  (MinIO endpoint)
  ├── raw_events/
  │   └── {topic}/
  │       ├── data/  (Parquet, Snappy, partitioned by event_date)
  │       └── metadata/
  └── aggregated_events/
      └── {domain}/{window_type}/
          ├── data/  (Z-order: entity_id + event_date)
          └── metadata/

CLOUD TIER — Object Storage (aged out from MinIO)
  MinIO lifecycle rule: objects > 90 days → replicate to S3/ADLS/GCS
  s3://{company}-cold-archive/streaming-iceberg/  (mirrored path)
  → queryable via Trino Iceberg connector pointing to cloud catalog
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Kafka ACLs + Cassandra RBAC + Apache Ranger + MinIO IAM         │
│                                                                  │
│  Principal             Access Level     Scope                    │
│  ─────────────────── ─ ─────────────    ──────────────────────  │
│  producer-svc          WRITE            own Kafka topics         │
│  flink-job-sa          READ (all topics) Kafka consumer group    │
│                        WRITE             Cassandra + MinIO       │
│  debezium-sa           READ              source DB (CDC)         │
│                        WRITE             cdc.* Kafka topics      │
│  cassandra-api-svc     SELECT            own keyspace            │
│  trino-query-svc       SELECT            Iceberg (Ranger filter) │
│  analyst-role          SELECT (cols)     Iceberg via Ranger RLS  │
│  ml-engineer-role      READ              MinIO + cloud archive   │
│                                                                  │
│  Kafka mTLS         → Strimzi cert manager, inter-broker TLS     │
│  Kafka Authz        → OPA / custom authorizer (fine-grained)     │
│  Cassandra RBAC     → keyspace + table + column level            │
│  MinIO IAM          → bucket policies per service account        │
│  Apache Ranger      → Iceberg column masking + row filtering      │
│  K8s RBAC           → service account per Flink JobManager       │
│  HashiCorp Vault    → secret injection into pods (CSI driver)    │
│  Network Policies   → K8s NetworkPolicy per namespace            │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    EV1[📡 Kafka topic\nreceives on-prem event]
    EV2[⏰ Flink watermark\nwindow boundary]
    EV3[🚨 CEP pattern fires]

    EV1 --> F1[Flink Job: enrich\nCassandra Async I/O]
    F1 --> F2[Flink Job: window aggregate]
    EV2 --> F2
    F2 --> W1[Cassandra Write\nhot aggregate]
    F2 --> W2[Iceberg Write on MinIO\nFlink Iceberg sink commit]
    EV1 -.->|Kafka Connect MinIO Sink| W3[MinIO raw archive]

    EV3 --> A1[Kafka alerts topic]
    A1 --> A2[Alertmanager\n→ PagerDuty / OpsGenie]

    W2 -->|Nessie snapshot| C1[Trino auto-picks up\nnew Iceberg partitions]

    W3 -->|MinIO lifecycle rule| CLOUD[Replicate to\ncloud object storage]
    CLOUD --> ML[Cloud ML training\njob triggered]

    W2 -->|weekly Airflow DAG| COMP[Spark compaction\nIceberg z-order + expire snapshots]

    F1 -->|TM failure| CKPT[Flink checkpoint on MinIO\n→ restart from last checkpoint]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Event Broker | Apache Kafka via Strimzi Operator | K8s-native; TLS cert via cert-manager; ZooKeeper or KRaft mode |
| CDC Capture | Debezium via Kafka Connect | Oracle / PostgreSQL / MySQL / SQL Server on-prem |
| Schema Registry | Confluent Schema Registry (K8s pod) | Avro; compatibility enforcement; backed by Kafka topic |
| Kafka Replication | MirrorMaker 2 | Active-passive to cloud Kafka for DR / burst |
| Stream Processor | Apache Flink (Flink Kubernetes Operator) | RocksDB state; checkpoints on MinIO S3A; exactly-once |
| State Backend | RocksDB + MinIO checkpoints (S3A) | NVMe SSDs on K8s nodes for RocksDB |
| Hot Store | Apache Cassandra (K8ssandra Operator) | StatefulSet on K8s; local RF=3; Cassandra 4.x |
| Cold Store | Apache Iceberg on MinIO | S3A connector; Parquet + Snappy; Flink `iceberg-flink` |
| Object Storage | MinIO (K8s) | S3-compatible; erasure coding; lifecycle policies |
| Iceberg Catalog | Project Nessie (K8s pod) | REST + git-like branching; JDBC backend (PostgreSQL) |
| Query Engine | Trino on K8s | Iceberg + Cassandra + Kafka connectors; on-prem queries |
| Access Control | Apache Ranger (K8s) | Column masking + row filter policies on Iceberg |
| BI Layer | Apache Superset (K8s) | Trino connection; on-prem dashboards |
| Orchestration | Apache Airflow (K8s CeleryExecutor) | Flink job submission, compaction, cloud sync |
| Monitoring | Prometheus + Grafana (K8s) | Flink, Kafka, Cassandra, MinIO, Trino metrics |
| Secrets | HashiCorp Vault + K8s CSI driver | Dynamic secrets; PKI for Kafka TLS |
| Cloud Tiering | MinIO → S3/ADLS/GCS lifecycle | Automatic replication after configurable retention |

---

## Comparison vs 4.8 (Multi-Cloud OSS Portable)

| Dimension | 4.9 Hybrid Self-Hosted | 4.8 Multi-Cloud OSS |
|-----------|----------------------|-------------------|
| Deployment model | On-prem K8s + cloud burst | Cloud K8s (any cloud) |
| Storage | MinIO on-prem → cloud lifecycle | Cloud object storage direct |
| Kafka deployment | Strimzi on bare-metal/VM K8s | Cloud-managed MSK / Event Hubs |
| Data residency | On-prem (hard requirement met) | Cloud (jurisdiction dependent) |
| DR / Burst | MirrorMaker 2 to cloud Kafka | Multi-region cloud natively |
| Infra ownership | Full (hardware + software) | Cloud hardware (managed) |
| Ops complexity | Highest | High |
| Latency | 1–10ms on-prem (local) | 10–100ms (network hop) |
| Data sovereignty | Maximum control | Cloud provider dependent |

---

## When to Choose This Implementation

✅ Strict data residency / sovereignty — data must not leave on-prem (banking, defense, healthcare)
✅ Existing on-prem Kubernetes infrastructure to leverage
✅ Ultra-low latency (<10ms) stream processing on local network
✅ Air-gapped or restricted environments where cloud not permitted for primary data
✅ Regulatory requirement for full infrastructure auditability

❌ No on-prem Kubernetes experience or platform team → use 4.8 (cloud OSS)
❌ Data residency not a constraint → use 4.8 or 4.7 (lower ops burden)
❌ Cloud-native team with no on-prem footprint → use 4.1/4.3/4.5
