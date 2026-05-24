---
layout: default
title: "3.9 — Data Lakehouse · Hybrid OSS Self-Hosted"
---

# 3.9 — Data Lakehouse · Hybrid OSS Self-Hosted

**Stack:** MinIO · Apache Iceberg · Apache Spark on Kubernetes · dbt Core · Apache Airflow · Trino · Project Nessie · Apache Ranger
**Processing:** Batch + Streaming · ACID Transactions · On-Prem + Cloud Hybrid · Full Build
**Buy vs Build:** Full Build (self-hosted on-prem / private cloud + cloud object store for spill)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES  (on-prem + cloud)                                            │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │On-prem   │  │On-prem   │  │  Files / │  │  Apache  │  │  Cloud   │    │
│  │Oracle /  │  │SAP ERP / │  │  NFS /   │  │  Kafka   │  │  SaaS    │    │
│  │SQL Server│  │  SFTP    │  │  SMB     │  │ (on K8s) │  │  (REST)  │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        │             │             │              │             │
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER  (on-prem Kubernetes)                                      │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  Debezium       │   │  Airbyte        │   │  Kafka Connect  │          │
│  │  on K8s         │   │  on K8s         │   │  + Spark        │          │
│  │  (CDC → Kafka)  │   │  (batch / SaaS) │   │  Structured     │          │
│  │                 │   │                 │   │  Streaming      │          │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘          │
└───────────┼────────────────────┼─────────────────────┼────────────────────┘
            └────────────────────┼─────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE — MinIO (on-prem S3-compatible) + Cloud spill (S3/ADLS/GCS)       │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  BRONZE         │   │   SILVER        │   │   GOLD          │          │
│  │  minio://bronze/│──▶│  minio://silver/│──▶│  minio://gold/  │          │
│  │  (on-prem hot)  │   │  (on-prem hot)  │   │  (on-prem/cloud)│          │
│  │                 │   │                 │   │                 │          │
│  │ • Iceberg ACID  │   │ • Spark MERGE   │   │ • dbt Core      │          │
│  │ • metadata/     │   │ • dbt Core      │   │ • Aggregates    │          │
│  │ • data/*.parquet│   │ • Deduped       │   │ • Kimball/Wide  │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
│                                                                             │
│  Tiering: Bronze → archive to cloud S3/ADLS/GCS after retention period    │
│  MinIO erasure coding + replication across on-prem nodes                  │
└─────────────────────────────────────────────────────────────────────────────┘
        ┆ (commit to Nessie)      ┆ (commit to Nessie)     ┆ (commit to Nessie)
        ▼                         ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Project Nessie on K8s + Apache Atlas + Apache Ranger             │
│  ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄   │
│  Nessie → versioned Iceberg catalog; prod / dev branch isolation            │
│  Apache Atlas → data lineage · PII classification · governance policies    │
│  Apache Ranger → column/row RBAC for Trino + Spark queries                 │
│  ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄   │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ versioned table lookup + Ranger access check
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
┌─────────────────┐   ┌──────────────────┐   ┌──────────────────────────────┐
│ CONSUMPTION     │   │ CONSUMPTION      │   │ CONSUMPTION                  │
│ — Ad-hoc SQL    │   │ — BI / Reporting │   │ — ML / Science               │
│                 │   │                  │   │                              │
│ Trino on K8s    │   │ Apache Superset  │   │ MLflow + Spark on K8s        │
│ (Nessie + Icebg │   │ (on K8s)         │   │ (reads Silver from MinIO)    │
│  connector)     │   │                  │   │                              │
└─────────────────┘   └──────────────────┘   └──────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Sources (on-prem)"]
        A1[(Oracle / SQL Server\non-prem)]
        A2[SAP ERP / SFTP\nfiles]
        A3[Files / NFS / SMB]
        A4[Apache Kafka\non K8s]
    end

    subgraph Ingestion["Ingestion (K8s pods)"]
        B1[Debezium on K8s\nCDC → Kafka]
        B2[Airbyte on K8s\nbatch → Iceberg]
        B3[Spark Structured Streaming\nKafka → Iceberg Bronze]
    end

    subgraph MinIO["MinIO (on-prem) — Iceberg Medallion Zones"]
        C1[🥉 Bronze\nminio://bronze/\nmetadata + data]
        C2[🥈 Silver\nminio://silver/\nmetadata + data]
        C3[🥇 Gold\nminio://gold/\nmetadata + data]
    end

    subgraph Catalog["Nessie · Apache Atlas · Apache Ranger (K8s)"]
        D1[Project Nessie\nversioned Iceberg catalog]
        D2[Apache Ranger\nRBAC enforcement]
    end

    subgraph Consume
        E1[Trino on K8s\nAd-hoc SQL]
        E2[Apache Superset\nDashboards]
        E3[MLflow + Spark\nML Training]
        E4[dbt Core\nfurther models]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B2
    A4 --> B3 --> C1

    C1 -->|Spark MERGE INTO\n+ dbt Core| C2
    C2 -->|Spark MERGE INTO\n+ dbt Core| C3

    C1 -.->|commit to Nessie| D1
    C2 -.->|commit to Nessie| D1
    C3 -.->|commit to Nessie| D1
    D1 -.-> D2

    D2 -.->|access check| C2
    D2 -.->|access check| C3

    C2 --> E1
    C2 --> E3
    C3 --> E1
    C3 --> E2
    C3 --> E4
```

---

## Zone Design

```
minio://<tenant>-lakehouse/       ← on-prem MinIO (S3-compatible API)
│
├── bronze/
│   └── {source}/{table}/
│       ├── metadata/
│       │   ├── 00000-*.metadata.json   ← Iceberg table metadata
│       │   └── snap-*.avro
│       └── data/
│           └── {year}/{month}/{day}/
│               └── *.parquet
│       TTL: 90 days → tiered to s3://<archive-bucket>/bronze/
│
├── silver/
│   └── {domain}/{entity}/
│       ├── metadata/
│       └── data/
│           └── {hidden-partition}/
│               └── *.parquet
│
└── gold/
    └── {domain}/{dbt_model}/
        ├── metadata/
        └── data/
            └── *.parquet               ← dims, facts, wide tables
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│    Apache Ranger · Nessie · MinIO IAM · HashiCorp Vault  │
│                                                           │
│  Ranger Principal / SA    Access Level    Zone(s)        │
│  ─────────────────────    ─────────────   ──────────── │
│  debezium-sa              Write only      Bronze         │
│  airbyte-sa               Write only      Bronze         │
│  spark-transform-sa       Read + Write    Bronze→Silver  │
│  dbt-core-sa              Read + Write    Silver→Gold    │
│  trino-sa                 Read only       Silver + Gold  │
│  data-engineer-ldap       Read + Write    All zones      │
│  data-analyst-ldap        Read only       Gold only      │
│  data-scientist-ldap      Read only       Silver + Gold  │
│                                                           │
│  MinIO policies     → bucket-level IAM (S3-compatible)   │
│  Ranger column mask → PII columns in Silver              │
│  Ranger row filter  → per team / cost centre tag         │
│  HashiCorp Vault    → DB creds + MinIO access keys       │
│  Nessie branches    → prod / dev / staging isolation     │
│  LDAP / AD          → principal source for Ranger        │
│  TLS everywhere     → MinIO + Nessie + Trino + Airflow   │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow DAG Schedule\ndaily 01:00 UTC]
    T2[📡 Kafka Consumer Lag\nthreshold trigger]

    T1 --> J1[Spark on K8s\nSource → Bronze\nIceberg MERGE via Nessie]
    T2 --> J1

    J1 --> NB[Nessie commit\nbronze table snapshot]
    NB --> J2[Spark on K8s / dbt Core\nBronze → Silver\nMERGE + dedup + cleanse]
    J2 --> DQ[dbt test suite\nnot_null · unique · referential]
    DQ --> J3[Spark on K8s / dbt Core\nSilver → Gold\nMERGE + aggregates]
    J3 --> J4[Iceberg Maintenance\nexpire_snapshots\nrewrite_data_files]
    J4 --> J5[MinIO Lifecycle\ntier Bronze to cloud archive]
    J5 --> N1[Airflow alert\n→ Slack / PagerDuty]

    J1 -->|fail| A1[Airflow alert\n→ Slack + PagerDuty]
    J2 -->|fail| A1
    J3 -->|fail| A1
    DQ -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| On-Prem Object Store | MinIO | S3-compatible API; erasure coding; multi-node HA |
| Cloud Archive Tier | S3 / ADLS / GCS | Bronze TTL overflow; Iceberg table still readable |
| Table Format | Apache Iceberg v2 | ACID, hidden partitioning, V2 row-level deletes |
| Versioned Catalog | Project Nessie | Git-like branches for catalog; runs on K8s |
| Data Lineage | Apache Atlas | Runs on K8s; Spark + dbt hooks |
| Access Control | Apache Ranger | Column/row RBAC; integrates with LDAP/AD |
| CDC Ingestion | Debezium on K8s | Pods per connector; outputs to Kafka |
| Event Broker | Apache Kafka on K8s | Strimzi Operator; on-prem |
| SaaS / Batch Ingestion | Airbyte on K8s | 300+ connectors; targets MinIO Bronze |
| Stream Ingestion | Spark Structured Streaming | Kafka → Iceberg Bronze micro-batch |
| Batch Processing | Apache Spark on K8s | Spark Operator; MERGE INTO Iceberg |
| Transform Layer | dbt Core | Runs against Trino; Iceberg dbt adapter |
| Ad-hoc Query | Trino on K8s | Nessie + Iceberg connector; Ranger plugin |
| Dashboards | Apache Superset on K8s | Connects to Trino |
| ML Consumption | MLflow + Apache Spark | Reads Silver; MLflow on K8s |
| Secret Management | HashiCorp Vault | DB creds, MinIO keys, TLS certs |
| Orchestration | Apache Airflow on K8s | KubernetesExecutor; DAGs in Git |
| Table Maintenance | Iceberg `expire_snapshots` + `rewrite_data_files` | Scheduled Airflow task via Spark |
| Infrastructure | Kubernetes (on-prem bare metal or VMware) | All components run as K8s workloads |
| Monitoring | Prometheus + Grafana + Spark History Server | Full on-prem observability stack |

---

## Comparison vs 3.8 — Multi-Cloud OSS Portable

| Dimension | 3.8 Multi-Cloud OSS | 3.9 Hybrid Self-Hosted |
|-----------|---------------------|------------------------|
| Object Storage | S3 / ADLS / GCS (cloud) | MinIO on-prem + cloud archive spill |
| Data Residency | Cloud region | On-prem datacenter (full control) |
| Catalog | Project Nessie (same) | Project Nessie on K8s (same, self-hosted) |
| Table Format | Apache Iceberg (same) | Apache Iceberg (same) |
| Compute | Cloud-managed clusters | Kubernetes on bare-metal / VMware |
| Network Egress Cost | Cloud object store egress | Near-zero (on-prem → on-prem) |
| Latency (internal) | Cloud WAN latency | On-prem sub-ms latency |
| Compliance | Cloud region boundaries | Full data residency on-prem |
| BI Layer | Apache Superset (same) | Apache Superset (same, on K8s) |
| ML | MLflow + Spark (same) | MLflow + Spark on K8s (same) |
| Cold Storage | Cloud storage classes | MinIO tiering → cloud S3/ADLS/GCS |
| Operational Overhead | Very high (OSS on cloud) | Extreme (OSS + bare-metal + K8s) |
| Skill Requirement | Spark + K8s + Iceberg + dbt | Above + datacenter / storage operations |
| Cost Model | Cloud compute + storage | CapEx (hardware) + OpEx (operations) |
| Best For | Max portability, avoid egress | Regulated industries, data sovereignty |
