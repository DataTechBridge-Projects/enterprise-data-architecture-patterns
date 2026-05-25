---
layout: default
title: "7.6 — Operational Analytics · GCP OSS on Cloud"
---

# 7.6 — Operational Analytics · GCP OSS on Cloud

**Stack:** PostgreSQL · Debezium · Pub/Sub · Apache Flink on GKE · Apache Iceberg on GCS · Trino · Apache Superset
**Processing:** Streaming-first · sub-15-second latency
**Buy vs Build:** Build (OSS on GCP cloud infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources"]
        S1[PostgreSQL on CloudSQL/GCE\nOrders · CRM]
        S2[MySQL on CloudSQL\nLegacy ERP]
        S3[MongoDB Atlas GCP\nProduct catalog]
        S4[App Services\nREST event stream]
    end

    subgraph CDC["CDC Layer — Debezium on GKE"]
        I1[Debezium PG Connector]
        I2[Debezium MySQL Connector]
        I3[Debezium MongoDB Connector]
    end

    subgraph BROKER["Google Cloud Pub/Sub"]
        K1[Topic: cdc.postgres.*]
        K2[Topic: cdc.mysql.*]
        K3[Topic: app.events]
    end

    subgraph PROC["Stream Processing — Apache Flink on GKE"]
        P1[Flink Upsert Job\nIceberg MERGE]
        P2[Flink Enrichment Job]
        P3[Flink Window Agg\nrolling metrics]
    end

    subgraph STORAGE["ODS — Apache Iceberg on GCS"]
        O1[iceberg.ods\ncurrent state]
        O2[iceberg.agg\nrolling aggregates]
        O3[iceberg.history\nfull CDC log]
    end

    subgraph CATALOG["Catalog\nGoogle Cloud Data Catalog + Hive Metastore"]
        C1[Data Catalog\nlineage + tags]
        C2[Apache Ranger\nTrino RBAC]
    end

    subgraph CONSUME["Consumption"]
        F1[Superset\nOperational Dashboards]
        F2[Trino on GKE\nAd-hoc SQL]
        F3[App API\nIceberg REST]
        F4[Flink CEP\nReal-time alerts]
    end

    SRC --> CDC
    I1 --> BROKER
    I2 --> BROKER
    I3 --> BROKER
    BROKER --> PROC
    P1 --> O1
    P2 --> O1
    P3 --> O2
    BROKER --> O3
    O1 & O2 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1
    C2 --> F2
    O1 --> F3
    PROC --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(PostgreSQL)]
        A2[(MySQL)]
        A3[(MongoDB)]
        A4[App REST Events]
    end

    subgraph Debezium["Debezium on GKE"]
        B1[PG Connector]
        B2[MySQL Connector]
        B3[MongoDB Connector]
        B4[HTTP Source]
    end

    subgraph PubSub["Cloud Pub/Sub Topics"]
        C1[cdc.* topics]
        C2[app.events]
    end

    subgraph Flink["Apache Flink on GKE"]
        D1[Upsert → Iceberg ODS]
        D2[Enrichment join]
        D3[Window Agg → Iceberg AGG]
    end

    subgraph Iceberg["Iceberg on GCS"]
        E1[ods tables]
        E2[agg tables]
    end

    subgraph Catalog["Data Catalog + Ranger"]
        F1[Catalog + Lineage]
        F2[RBAC Policies]
    end

    subgraph Consume
        G1[Trino SQL]
        G2[Superset Dashboards]
        G3[App REST API]
        G4[Flink CEP Alerts]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> B4 --> C2

    C1 --> D1 --> E1
    C1 --> D2 --> E1
    C1 & C2 --> D3 --> E2

    E1 & E2 -->|scan| F1 --> F2

    F2 -->|reads| G1
    F2 -->|reads| G2
    E1 -->|REST| G3
    C1 --> G4
```

---

## Zone Design

```
GCS: gs://<company>-ops-iceberg/
│
├── ods/
│   └── {domain}/{entity}/             -- Iceberg table (current state)
│       MOR (Merge-on-Read) for low-latency upserts
│       Flink upsert on primary key
│
├── agg/
│   └── {domain}/{metric}/{grain}/     -- Iceberg table (rolling aggregates)
│       Flink tumbling window: 1-min, 5-min, 1-hr
│
└── history/
    └── cdc/{topic}/year=YYYY/month=MM/day=DD/
        Raw Pub/Sub CDC events; TTL 90 days via GCS lifecycle
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│         Apache Ranger + IAM + GCS ACLs                │
│                                                       │
│  GCP SA / Group          Access Level  Layer          │
│  ──────────────────────  ────────────  ──────────     │
│  debezium-sa             Publish only  Pub/Sub        │
│  flink-job-sa            Read + Write  Pub/Sub + GCS  │
│  ops-engineer@           Read + Write  All Iceberg    │
│  data-analyst@           Read only     ods · agg      │
│  app-service-sa          Read only     ods (cols)     │
│  superset-sa             Read only     agg            │
│                                                       │
│  Column masking → Ranger masking policies on Trino    │
│  Row filtering  → Trino row-level filter in Ranger    │
│  Encryption     → CMEK (Cloud KMS) on GCS buckets     │
│  mTLS           → Pub/Sub HTTPS + GKE Workload ID     │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Debezium\ncontinuous CDC]
    T2[⚡ Flink Jobs\nalways-on streaming]
    T3[⏰ Cloud Composer DAG\nhourly Iceberg compaction]
    T4[⏰ Cloud Composer DAG\ndaily VACUUM + TTL]

    T1 -->|change events| K1[Pub/Sub Topics]
    K1 --> F1[Flink Upsert Job\n→ Iceberg ODS]
    K1 --> F2[Flink Window Agg\n→ Iceberg AGG]
    K1 --> F3[Flink CEP\nalerts → Pub/Sub → Cloud Run]

    T3 --> C1[Iceberg OPTIMIZE\nsmall-file merge via Spark on Dataproc]
    T4 --> C2[Iceberg EXPIRE_SNAPSHOTS\nVACUUM old files]

    F1 -->|lag > 30s| A1[Cloud Monitoring Alert\n→ PagerDuty]
    F2 -->|fail| A1
    C1 -->|fail| A2[Composer Alert → Slack]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| OLTP Sources | PostgreSQL / MySQL / MongoDB on GCP | WAL / binlog / oplog enabled |
| CDC Engine | Debezium on GKE (Kafka Connect) | Connector pods per source type |
| Message Broker | Google Cloud Pub/Sub | Managed; Kafka-compatible via Pub/Sub Lite |
| Stream Processor | Apache Flink on GKE | Upsert, enrichment, window agg |
| ODS Store | Apache Iceberg on GCS | MOR tables; PK upsert |
| Aggregate Store | Apache Iceberg on GCS | Tumbling window output |
| Table Catalog | Google Cloud Data Catalog + HMS | Auto-scan; Iceberg catalog backend |
| Access Control | Apache Ranger on Trino | RBAC + column masking |
| Query Engine | Trino on GKE | Iceberg connector |
| BI / Dashboards | Apache Superset | Connects via Trino |
| App-facing API | Iceberg REST Catalog + Trino | HTTP query for microservices |
| CEP Alerts | Flink CEP | Pattern match → Pub/Sub → Cloud Run |
| Compaction | Iceberg + Spark on Dataproc | Hourly via Cloud Composer |
| Orchestration | Cloud Composer (Airflow) | Maintenance + compaction DAGs |
| Monitoring | Cloud Monitoring + Prometheus + Grafana | Flink, GKE, Pub/Sub metrics |

---

## Comparison vs 7.5 (GCP Managed)

| Dimension | 7.6 GCP OSS | 7.5 GCP Fully Managed |
|-----------|------------|----------------------|
| CDC Engine | Debezium | Datastream |
| Message Bus | Pub/Sub | None (Datastream direct) |
| Stream Processor | Apache Flink | Dataflow (Beam) |
| ODS Store | Iceberg on GCS | BigQuery |
| Latency | ~5–15 s | ~1–5 min |
| Open Format | Yes (Iceberg) | No (BigQuery proprietary) |
| Ops Overhead | High (GKE + Debezium) | Low |
| Cost at Scale | Lower (GCS storage) | Higher (BigQuery slot cost) |

---

## When to Choose This Implementation

✅ Need sub-15-second end-to-end latency  
✅ Open table format required (vendor portability)  
✅ Sources outside GCP-native (on-prem PostgreSQL, external MySQL)  
✅ Team has Flink + Kafka/Pub/Sub expertise  
✅ Trino multi-engine consumption required  

❌ No Flink expertise → use 7.5 (Datastream + Dataflow)  
❌ Looker on BigQuery preferred → use 7.5  
❌ Fully managed preference → use 7.5
