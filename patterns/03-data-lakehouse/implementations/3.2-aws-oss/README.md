---
layout: default
title: "2.2 — Data Lakehouse · AWS OSS on Cloud"
---

# 3.2 — Data Lakehouse · AWS OSS on Cloud

**Stack:** S3 · Apache Iceberg · Spark on EMR · dbt Core · Apache Airflow (MWAA) · Trino
**Processing:** Batch + Streaming · ACID Transactions · Hidden Partitioning · Open Catalog
**Buy vs Build:** Build (OSS stack on managed AWS infra)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                               │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │PostgreSQL│  │ SaaS APIs│  │  Files   │  │  Apache  │  │   IoT /  │    │
│  │ MySQL /  │  │ REST /   │  │(CSV/JSON │  │  Kafka   │  │  Events  │    │
│  │ MongoDB  │  │ JDBC     │  │  Parquet)│  │  (MSK)   │  │  (MSK)   │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        │             │             │              │             │
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                            │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  Debezium       │   │  Apache Spark   │   │  Spark          │          │
│  │  on MSK Connect │   │  on EMR         │   │  Structured     │          │
│  │  (CDC → Kafka)  │   │  (batch ingest) │   │  Streaming      │          │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘          │
└───────────┼────────────────────┼─────────────────────┼────────────────────┘
            └────────────────────┼─────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE — Amazon S3  (Apache Iceberg table format)                         │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  BRONZE         │   │   SILVER        │   │   GOLD          │          │
│  │  s3://bronze/   │──▶│  s3://silver/   │──▶│  s3://gold/     │          │
│  │                 │   │                 │   │                 │          │
│  │ • Raw Iceberg   │   │ • dbt Core      │   │ • dbt Core      │          │
│  │ • Spark APPEND  │   │ • Spark MERGE   │   │ • Spark MERGE   │          │
│  │ • metadata/     │   │ • Deduped       │   │ • Kimball/wide  │          │
│  │ • data/*.parquet│   │ • Type-cast     │   │ • BI-ready      │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
│                                                                             │
│  Iceberg V2: row-level deletes · partition evolution · snapshot isolation  │
└─────────────────────────────────────────────────────────────────────────────┘
        ┆ (commit snapshot)       ┆ (commit snapshot)      ┆ (commit snapshot)
        ▼                         ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — AWS Glue Data Catalog (Iceberg REST) + Lake Formation            │
│  ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄   │
│  Iceberg REST catalog → Spark, Trino resolve table metadata + snapshots    │
│  Glue Data Catalog → persists Iceberg table metadata, schemas, locations   │
│  Lake Formation → fine-grained column/row access per IAM role              │
│  ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄ ┄   │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ snapshot resolution + access check
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
┌─────────────────┐   ┌──────────────────┐   ┌──────────────────────────────┐
│ CONSUMPTION     │   │ CONSUMPTION      │   │ CONSUMPTION                  │
│ — Interactive   │   │ — BI / Reporting │   │ — ML / Science               │
│                 │   │                  │   │                              │
│ Trino on EMR    │   │ Amazon Athena v3 │   │ SageMaker                    │
│ (low-latency    │   │ (serverless SQL  │   │ (EMR Spark reads Silver      │
│  SQL on Iceberg)│   │  on Gold)        │   │  Iceberg for training)       │
│                 │   │ Apache Superset  │   │                              │
│                 │   │ (dashboards)     │   │                              │
└─────────────────┘   └──────────────────┘   └──────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(PostgreSQL / MySQL)]
        A2[SaaS REST APIs]
        A3[Files / S3 Drop]
        A4[MSK / Kafka Topics]
    end

    subgraph Ingestion
        B1[Debezium + MSK Connect\nCDC → Kafka]
        B2[Spark on EMR\nbatch ingest]
        B3[Spark Structured Streaming\nmicro-batch → Iceberg]
    end

    subgraph S3_Iceberg["S3 — Iceberg Medallion Zones"]
        C1[🥉 Bronze\ns3://bronze/\nmetadata + data]
        C2[🥈 Silver\ns3://silver/\nmetadata + data]
        C3[🥇 Gold\ns3://gold/\nmetadata + data]
    end

    subgraph Catalog["Glue Catalog · Iceberg REST · Lake Formation"]
        D1[Iceberg REST Catalog\nGlue endpoint]
        D2[Lake Formation\nACL + Column Mask]
    end

    subgraph Consume
        E1[Trino on EMR\nAd-hoc SQL]
        E2[Athena v3\nServerless SQL]
        E3[SageMaker\nML Training]
        E4[Apache Superset\nDashboards]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B2
    A4 --> B3 --> C1

    C1 -->|Spark MERGE INTO\n+ dbt Core models| C2
    C2 -->|Spark MERGE INTO\n+ dbt Core models| C3

    C1 -.->|commit snapshot| D1
    C2 -.->|commit snapshot| D1
    C3 -.->|commit snapshot| D1
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
s3://<company>-lakehouse/
│
├── bronze/
│   └── {source}/{table}/
│       ├── metadata/
│       │   ├── 00000-*.metadata.json
│       │   └── snap-*.avro
│       └── data/
│           └── {year=YYYY}/{month=MM}/{day=DD}/
│               └── *.parquet
│
├── silver/
│   └── {domain}/{entity}/
│       ├── metadata/
│       └── data/
│           └── {hidden-partition}/
│               └── *.parquet           ← deduped, cleansed, typed
│
└── gold/
    └── {domain}/{dbt_model}/
        ├── metadata/
        └── data/
            └── *.parquet               ← dbt-built dims, facts, wide tables
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│         Lake Formation · IAM · KMS                        │
│                                                           │
│  IAM Role / Principal   Access Level    Zone(s)          │
│  ─────────────────────  ─────────────   ──────────────── │
│  emr-ingest-role        Write only      Bronze           │
│  spark-streaming-role   Write only      Bronze           │
│  dbt-core-role          Read + Write    Silver + Gold    │
│  trino-service-acct     Read only       Silver + Gold    │
│  data-engineer          Read + Write    All zones        │
│  data-analyst           Read only       Gold only        │
│  data-scientist         Read only       Silver + Gold    │
│  sagemaker-role         Read only       Silver + Gold    │
│                                                           │
│  Iceberg V2 row deletes → GDPR erasure without rewrite   │
│  Lake Formation column masks → PII in Silver             │
│  KMS per zone → Bronze / Silver / Gold separate CMKs     │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ MWAA DAG Schedule\ndaily 01:00 UTC]
    T2[📡 MSK Event\nnew CDC batch available]

    T1 --> J1[EMR Step\nSource → Bronze\nSpark Iceberg MERGE]
    T2 --> J1

    J1 --> J2[EMR Step / dbt Core\nBronze → Silver\nMERGE + dedup + cleanse]
    J2 --> DQ[dbt Test Suite\nnot_null · unique · accepted_values]
    DQ --> J3[EMR Step / dbt Core\nSilver → Gold\nMERGE + aggregates]
    J3 --> J4[Iceberg Maintenance\nexpire_snapshots\nrewrite_data_files]
    J4 --> N1[SNS Alert\npipeline complete]

    J1 -->|fail| A1[CloudWatch Alarm\n→ SNS → PagerDuty]
    J2 -->|fail| A1
    J3 -->|fail| A1
    DQ -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | Amazon S3 | Iceberg metadata + Parquet data files |
| Table Format | Apache Iceberg v2 | ACID, hidden partitioning, V2 row-level deletes |
| CDC Ingestion | Debezium + Amazon MSK Connect | Streams DB changes to Kafka/MSK topics |
| Batch Ingestion | Apache Spark on EMR 6.x | EMR with `iceberg-spark-runtime` jar |
| Stream Ingestion | Spark Structured Streaming on EMR | Micro-batch Iceberg writes |
| Schema Catalog | AWS Glue Data Catalog (Iceberg REST) | Spark/Trino catalog configuration |
| Access Control | AWS Lake Formation | Column/row security on Glue Catalog tables |
| Transform Layer | dbt Core on EMR | Models compile to Spark SQL with Iceberg |
| Ad-hoc Query | Trino on EMR | Iceberg connector; sub-second interactive queries |
| Serverless Query | Amazon Athena v3 | Native Iceberg; cost-effective ad-hoc |
| Dashboards | Apache Superset | Connects to Trino or Athena |
| ML Consumption | Amazon SageMaker | Reads Silver via Spark on EMR |
| Orchestration | Amazon MWAA (Airflow 2.x) | DAGs submit EMR steps + dbt CLI tasks |
| Table Maintenance | Iceberg `expire_snapshots` + `rewrite_data_files` | Run as post-Gold EMR step |
| Encryption | AWS KMS | SSE-KMS per zone bucket |
| Monitoring | CloudWatch + MWAA UI | EMR step logs + Airflow task history |

---

## Comparison vs 3.1 — AWS Fully Managed

| Dimension | 3.1 AWS Fully Managed | 3.2 AWS OSS on Cloud |
|-----------|-----------------------|----------------------|
| Table Format | Apache Iceberg | Apache Iceberg (same) |
| Transform Tool | dbt Cloud (SaaS) | dbt Core (self-managed) |
| Processing Engine | AWS Glue Jobs (Serverless Spark) | Spark on EMR (cluster-based) |
| Ingestion CDC | AWS DMS | Debezium + MSK Connect |
| Interactive Query | Athena v3 (serverless) | Trino on EMR (lower latency) |
| Catalog | Glue Catalog managed | Glue Catalog + Iceberg REST config |
| Orchestration | dbt Cloud Scheduler | Amazon MWAA (Airflow) |
| Streaming Ingest | Kinesis Firehose | Spark Structured Streaming |
| Operational Overhead | Low (fully managed) | Higher (EMR + MWAA cluster ops) |
| Cost Model | Pay-per-use (Glue DPU + Athena) | EMR instance-hour + MWAA workers |
| Customisability | Constrained by Glue/DMS | Full OSS flexibility |
| Vendor Lock-in | Medium (AWS Glue, DMS) | Low (portable Iceberg + dbt) |
| Best For | Speed to production, managed ops | Cost control, OSS portability |
