---
layout: default
title: "7.5 — Operational Analytics · GCP Fully Managed"
---

# 7.5 — Operational Analytics · GCP Fully Managed

**Stack:** Cloud Spanner · Datastream (CDC) · BigQuery · Looker · Cloud Data Catalog
**Processing:** Streaming / Near-Real-Time · ODS-into-BigQuery pattern
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources"]
        S1[Cloud Spanner\nGlobal transactions]
        S2[Cloud SQL PostgreSQL\nOrders · CRM]
        S3[Cloud SQL MySQL\nLegacy apps]
        S4[Pub/Sub\nApp event stream]
    end

    subgraph CDC["CDC / Ingestion Layer"]
        I1[Datastream\nSpanner → BigQuery]
        I2[Datastream\nCloud SQL → BigQuery]
        I3[Dataflow\nPub/Sub → BigQuery Streaming]
    end

    subgraph ODS["ODS — BigQuery"]
        O1[Dataset: staging_*\nraw CDC rows]
        O2[Dataset: ods_*\ncurrent state views / tables]
        O3[Dataset: agg_*\nrolling aggregates]
    end

    subgraph CATALOG["Catalog & Governance\nGoogle Cloud Data Catalog + Dataplex"]
        C1[Data Catalog\ntable tags + lineage]
        C2[Dataplex\ndata policies + RBAC]
        C3[Cloud DLP\nPII detection]
    end

    subgraph CONSUME["Consumption"]
        F1[Looker\nOperational Dashboards]
        F2[BigQuery SQL\nAd-hoc queries]
        F3[BigQuery API\nApp-facing queries]
        F4[Cloud Monitoring\nOps alerts]
    end

    SRC --> CDC
    I1 --> O1
    I2 --> O1
    I3 --> O1
    O1 --> O2
    O2 --> O3
    O2 & O3 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1
    C2 --> F2
    O2 --> F3
    O3 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Cloud Spanner)]
        A2[(Cloud SQL PostgreSQL)]
        A3[(Cloud SQL MySQL)]
        A4[Pub/Sub Topics]
    end

    subgraph Ingestion
        B1[Datastream\nSpanner CDC]
        B2[Datastream\nCloud SQL CDC]
        B3[Dataflow\nStreaming pipeline]
    end

    subgraph BigQuery["BigQuery ODS Datasets"]
        C1[staging_*\nraw CDC rows]
        C2[ods_*\ncurrent state MERGE]
        C3[agg_*\nrolling metrics]
    end

    subgraph Catalog["Data Catalog + Dataplex"]
        D1[Data Catalog\nlineage + tags]
        D2[Dataplex Policy\nRBAC enforcement]
    end

    subgraph Consume
        E1[Looker\nDashboards + Explores]
        E2[BigQuery SQL\nAd-hoc]
        E3[BigQuery API\nApp queries]
        E4[Cloud Monitoring\nAlerts]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B2
    A4 --> B3 --> C1

    C1 -->|BigQuery Scheduled Query\nMERGE on PK| C2
    C2 -->|Scheduled Query\n5-min agg| C3

    C2 & C3 -->|auto-tag| D1 --> D2

    D2 -->|reads| E1
    D2 -->|reads| E2
    C2 -->|Storage Read API| E3
    C3 -->|threshold row| E4
```

---

## Zone Design

```
BigQuery project: <company>-ops-analytics
│
├── Dataset: staging_{source}
│   └── {table}                      -- raw CDC rows from Datastream
│       Partitioned by _metadata_timestamp
│
├── Dataset: ods_{domain}
│   └── {entity}                     -- current state (MERGE via Scheduled Query)
│       Partitioned by updated_at; clustered by primary key
│
└── Dataset: agg_{domain}
    └── {metric}_{grain}             -- rolling aggregates (5-min, 1-hr)
        Partitioned by window_start; Materialized Views where possible

GCS: gs://<company>-datastream-staging/
    └── {source}/{table}/            -- Datastream intermediate (auto-managed)
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│         Dataplex + IAM + BigQuery Column Security     │
│                                                       │
│  IAM Role / SA              Access Level  Dataset     │
│  ──────────────────────     ────────────  ──────────  │
│  datastream-sa              Write only    staging_*   │
│  dataflow-sa                Write only    staging_*   │
│  ops-engineer@              Read + Write  All         │
│  ops-analyst@               Read only     ods_* agg_* │
│  bi-consumer@               Read only     agg_*       │
│  app-service-sa             Read only     ods_* (cols)|
│                                                       │
│  Column security → BigQuery Policy Tags (Data Catalog)|
│  Row security    → BigQuery Row Access Policies       │
│  Encryption      → CMEK on all BigQuery datasets      │
│  VPC             → VPC-SC perimeter on project        │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Datastream\ncontinuous CDC]
    T2[⚡ Dataflow\nstreaming Pub/Sub]
    T3[⏰ BigQuery Scheduled Query\nevery 5 min]
    T4[⏰ Cloud Composer DAG\nhourly maintenance]

    T1 -->|change rows| J1[staging_* tables]
    T2 --> J1
    J1 --> J2[BigQuery Scheduled Query\nMERGE staging → ods]
    T3 --> J3[BigQuery Scheduled Query\nagg refresh CTAS]
    J3 --> J4[Materialized View Refresh\nBigQuery auto]

    T4 --> J5[Partition Expiry Check\nstaging TTL 7d]
    T4 --> J6[DLP Scan\nPII tagging on ods_*]

    J2 -->|fail| A1[Cloud Monitoring Alert\n→ PagerDuty / Cloud Chat]
    T1 -->|lag > 2 min| A1
```

---

## Component Map

| Component | GCP Service | Notes |
|-----------|------------|-------|
| OLTP Source (global) | Cloud Spanner | Datastream native connector |
| OLTP Source (relational) | Cloud SQL PostgreSQL / MySQL | Datastream CDC connector |
| App Events | Pub/Sub | Dataflow streaming ingest |
| CDC Engine | Datastream | Fully managed CDC; no Debezium needed |
| Stream Ingest | Dataflow (Apache Beam) | Pub/Sub → BigQuery streaming |
| ODS Store | BigQuery Datasets | staging + ods + agg; serverless |
| Merge Logic | BigQuery Scheduled Queries | MERGE on PK every 5 min |
| Aggregates | BigQuery Materialized Views | Incremental refresh; auto-managed |
| Catalog | Google Cloud Data Catalog | Auto-scan; policy tags for PII |
| Governance | Dataplex | Data policies + RBAC on datasets |
| PII Detection | Cloud DLP | Auto-tag sensitive columns |
| BI / Dashboards | Looker | LookML semantic model on BigQuery |
| Ad-hoc SQL | BigQuery SQL console / API | Serverless; pay per TB scanned |
| App-facing API | BigQuery Storage Read API | High-throughput row reads |
| Alerting | Cloud Monitoring + Alerting | Metric thresholds → PagerDuty |
| Encryption | CMEK (Cloud KMS) | Per-dataset key management |

---

## Comparison vs 7.6 (GCP OSS)

| Dimension | 7.5 GCP Fully Managed | 7.6 GCP OSS |
|-----------|----------------------|-------------|
| CDC Engine | Datastream | Debezium on GKE |
| Message Bus | None (Datastream direct) | Pub/Sub (Kafka API) |
| Stream Processor | Dataflow (Beam) | Apache Flink on GKE |
| ODS Store | BigQuery | Iceberg on GCS |
| Latency | ~1–5 min | ~5–15 s |
| Ops Overhead | Low | High |
| Cost Model | BigQuery slot + streaming | GKE + Pub/Sub + compute |
| Open Format | No (BigQuery proprietary) | Yes (Iceberg) |
| BI Tool | Looker | Superset |

---

## When to Choose This Implementation

✅ GCP is primary cloud with Cloud SQL / Spanner sources  
✅ Looker is the primary BI tool  
✅ BigQuery is already the analytical standard  
✅ Want zero CDC pipeline maintenance (Datastream)  
✅ Sub-5-minute latency is acceptable  

❌ Need sub-30-second latency → use 7.6 (Flink on GKE)  
❌ Open table format required → use 7.6 (Iceberg)  
❌ Sources outside GCP (on-prem PostgreSQL, MySQL) → use 7.6 or 7.8
