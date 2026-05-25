---
layout: default
title: "7.2 — Operational Analytics · AWS OSS on Cloud"
---

# 7.2 — Operational Analytics · AWS OSS on Cloud

**Stack:** PostgreSQL · Debezium · Amazon MSK (Kafka) · Apache Flink · Apache Iceberg · Trino · Superset
**Processing:** Streaming-first · sub-15-second latency
**Buy vs Build:** Build (cloud-native OSS, full control)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources"]
        S1[PostgreSQL\nOrders · Inventory · CRM]
        S2[MySQL / MariaDB\nLegacy apps]
        S3[MongoDB\nProduct catalog]
        S4[App Services\nREST events]
    end

    subgraph CDC["CDC Layer"]
        I1[Debezium Connector\non ECS / EC2]
        I2[Kafka Connect\nSource connectors]
    end

    subgraph BROKER["Amazon MSK — Kafka"]
        K1[Topic: cdc.orders]
        K2[Topic: cdc.inventory]
        K3[Topic: app.events]
    end

    subgraph PROC["Stream Processing — Apache Flink on EMR/KDA"]
        P1[Flink Job\nDeduplication + Upsert]
        P2[Flink Job\nEnrichment + Joins]
        P3[Flink Job\nAggregation Windows]
    end

    subgraph STORAGE["ODS — Apache Iceberg on S3"]
        O1[iceberg.ods\ncurrent state tables]
        O2[iceberg.agg\nrolling aggregates]
        O3[iceberg.history\nfull CDC log]
    end

    subgraph CATALOG["Catalog\nAWS Glue Data Catalog"]
        C1[Glue Catalog\nIceberg table registry]
        C2[Apache Ranger\nRBAC enforcement]
    end

    subgraph CONSUME["Consumption"]
        F1[Superset\nOperational Dashboards]
        F2[Trino\nAd-hoc SQL]
        F3[App API\nTrino or Iceberg REST]
        F4[Flink CEP\nReal-time alerts]
    end

    SRC --> CDC
    I1 --> BROKER
    I2 --> BROKER
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

    subgraph CDC
        B1[Debezium\nPostgreSQL Connector]
        B2[Debezium\nMySQL Connector]
        B3[Debezium\nMongoDB Connector]
        B4[Kafka Connect\nHTTP Source]
    end

    subgraph Kafka["Amazon MSK — Kafka Topics"]
        C1[cdc.* topics\nraw change events]
        C2[app.events\ntopic]
    end

    subgraph Flink["Flink Jobs on EMR / KDA"]
        D1[Upsert Job\nmerge by PK into Iceberg]
        D2[Enrichment Job\njoin dim tables]
        D3[Window Agg Job\n5-min rolling metrics]
    end

    subgraph Iceberg["Iceberg on S3"]
        E1[ods tables\ncurrent state]
        E2[agg tables\nmetrics]
    end

    subgraph Catalog["Glue Catalog + Ranger"]
        F1[Table Registry]
        F2[RBAC Policies]
    end

    subgraph Consume
        G1[Trino\nAd-hoc SQL]
        G2[Superset\nDashboards]
        G3[App API\nIceberg REST Catalog]
        G4[Flink CEP\nAlerts]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> B4 --> C2

    C1 --> D1 --> E1
    C1 --> D2 --> E1
    C1 & C2 --> D3 --> E2

    E1 & E2 -->|register| F1 --> F2

    F2 -->|reads| G1
    F2 -->|reads| G2
    E1 -->|REST catalog| G3
    C1 --> G4
```

---

## Zone Design

```
s3://<company>-ops-iceberg/
│
├── ods/
│   └── {domain}/{entity}/         -- Iceberg table (current state)
│       Flink upsert on primary key; MOR (Merge-on-Read) for low latency
│
├── agg/
│   └── {domain}/{metric}/{grain}/ -- Iceberg table (rolling aggregates)
│       Flink window: tumbling 1-min, 5-min, 1-hr
│
└── history/
    └── cdc/{topic}/year=YYYY/month=MM/day=DD/
        Raw Kafka CDC events archived; Iceberg partitioned by ingestion time
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│           Apache Ranger + IAM                         │
│                                                       │
│  Role                  Access Level    Layer          │
│  ──────────────────    ────────────    ──────────     │
│  debezium-connector    Produce only    MSK topics     │
│  flink-job-role        Read + Write    Kafka + S3     │
│  ops-engineer          Read + Write    All Iceberg    │
│  data-analyst          Read only       ods · agg      │
│  app-service           Read only       ods (row-level)|
│  superset-user         Read only       agg            │
│                                                       │
│  Column masking → Ranger masking policies on Trino    │
│  Row filtering  → Trino row-level filter in Ranger    │
│  Encryption     → KMS SSE-S3 on all Iceberg data      │
│  mTLS           → MSK inter-broker + client auth      │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Debezium\ncontinuous CDC stream]
    T2[⚡ Flink Jobs\nalways-on streaming]
    T3[⏰ Airflow DAG\nhourly Iceberg compaction]
    T4[⏰ Airflow DAG\ndaily partition expiry]

    T1 -->|change events| K1[MSK Kafka Topics]
    K1 --> F1[Flink Upsert Job\n→ Iceberg ODS]
    K1 --> F2[Flink Window Agg\n→ Iceberg AGG]
    K1 --> F3[Flink CEP\nfraud / anomaly alerts]

    T3 --> C1[Iceberg Compaction\nsmall file merge]
    T4 --> C2[Partition Expiry\nhistory TTL 90d]

    F1 -->|fail / lag >30s| A1[CloudWatch Alarm\n→ PagerDuty]
    F2 -->|fail| A1
    C1 -->|fail| A2[Airflow Alert\n→ Slack]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| OLTP Source | PostgreSQL / MySQL / MongoDB | Binlog / WAL / Oplog enabled |
| CDC Engine | Debezium on ECS / EC2 | Kafka Connect framework |
| Message Broker | Amazon MSK (Kafka 3.x) | Multi-AZ; m5.2xlarge brokers |
| Stream Processor | Apache Flink on EMR / KDA | Upsert, enrichment, window agg |
| ODS Store | Apache Iceberg on S3 | MOR tables; PK-based upsert |
| Aggregate Store | Apache Iceberg on S3 | Tumbling windows 1-min / 5-min |
| Table Catalog | AWS Glue Data Catalog | Iceberg catalog backend |
| Access Control | Apache Ranger | RBAC + column masking on Trino |
| Query Engine | Trino on EKS / EMR | Iceberg connector; ad-hoc SQL |
| BI / Dashboards | Apache Superset | Connects to Trino |
| App-facing API | Iceberg REST Catalog + Trino | HTTP query for microservices |
| CEP Alerts | Flink CEP | Pattern matching → SNS |
| Compaction | Iceberg + Spark/Flink job | Hourly small-file merge |
| Orchestration | Apache Airflow on MWAA | Compaction, maintenance DAGs |
| Monitoring | CloudWatch + Prometheus | Flink metrics, Kafka lag |

---

## Comparison vs 7.1 (AWS Managed)

| Dimension | 7.2 AWS OSS | 7.1 AWS Fully Managed |
|-----------|------------|----------------------|
| CDC Engine | Debezium | AWS DMS |
| Message Bus | Kafka (MSK) | None (DMS direct) |
| Stream Processor | Apache Flink | DMS + Redshift stored proc |
| ODS Store | Iceberg on S3 | Redshift schemas |
| Latency | ~5–15 s | ~60 s |
| Open Format | Yes (Iceberg) | No (Redshift proprietary) |
| Ops Overhead | High | Low |
| Cost at Scale | Lower (S3 storage) | Higher (Redshift nodes) |
| Multi-engine | Yes (Trino + Flink + Spark) | No (Redshift only) |

---

## When to Choose This Implementation

✅ Need sub-15-second end-to-end latency  
✅ Multi-engine consumption (Flink + Trino + Spark)  
✅ Open table format required (vendor portability)  
✅ Team has Kafka and Flink expertise  
✅ Large data volumes where S3+Iceberg is cheaper than Redshift  

❌ No Kafka/Flink expertise → use 7.1 (managed DMS + Redshift)  
❌ Need HTAP (same store for OLTP + OLAP) → use 7.7 (SingleStore)  
❌ Fully managed preference → use 7.1
