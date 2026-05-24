---
layout: default
title: "2.2 — Data Lake · AWS OSS on Cloud"
---

# 2.2 — Data Lake · AWS OSS on Cloud

**Stack:** S3 · Spark on EMR · Hive Metastore · Trino · Airflow · Superset
**Processing:** Batch-first · Schema-on-Read
**Buy vs Build:** Build (OSS on AWS managed infra — no vendor lock-in, higher ops overhead)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ RDS /    │  │ SaaS APIs│  │  Files   │  │  Kafka   │  │   IoT /  │   │
│  │ Aurora   │  │          │  │          │  │  Topics  │  │  Events  │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │  Debezium       │  │  Airbyte        │  │  Kafka Connect  │            │
│  │  (CDC via WAL)  │  │  (SaaS / Files) │  │  S3 Sink        │            │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘            │
└───────────┼────────────────────┼────────────────────┼────────────────────┘
            └────────────────────┼────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE — Amazon S3                                                        │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  LANDING ZONE   │──▶│   RAW ZONE      │──▶│  CURATED ZONE   │          │
│  │  s3://landing/  │   │  s3://raw/      │   │  s3://curated/  │          │
│  │  original files │   │  Parquet + Snappy│   │  clean Parquet  │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PROCESSING — Apache Spark on Amazon EMR                                    │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  EMR Cluster (auto-scaling)                                  │          │
│  │  Landing → Raw : convert formats, apply partitioning         │          │
│  │  Raw → Curated : deduplicate, type-cast, business rules      │          │
│  │  Triggered by Airflow DAGs                                   │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Hive Metastore (on RDS MySQL)                                    │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Stores table definitions, partition metadata, S3 locations  │          │
│  │  Compatible with Spark, Trino, EMR natively                  │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
           │ schema lookup → S3 location          │ schema lookup → S3 location
           ▼                                      ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  s3://raw/          │               │  s3://curated/                       │
└──────────┬──────────┘               └──────────────┬───────────────────────┘
           │                                         │
           ▼                                         ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  SageMaker / Spark  │               │  Trino       → ad-hoc SQL            │
│  (ML training)      │               │  Superset    → dashboards            │
│                     │               │  Spark       → further processing    │
└─────────────────────┘               └──────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[SaaS APIs]
        A3[Kafka Topics]
    end

    subgraph Ingestion
        B1[Debezium\nCDC]
        B2[Airbyte\nConnectors]
        B3[Kafka Connect\nS3 Sink]
    end

    subgraph S3["S3 — Storage Zones"]
        C1[🪣 Landing]
        C2[🪣 Raw\nParquet]
        C3[🪣 Curated\nParquet]
    end

    subgraph Catalog["Hive Metastore (RDS MySQL)"]
        D1[Table Definitions\nPartition Locations]
    end

    subgraph Consume
        E1[Trino\nAd-hoc SQL]
        E2[Superset\nDashboards]
        E3[SageMaker\nML Training]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1

    C1 -->|EMR Spark Job\nconvert + partition| C2
    C2 -->|EMR Spark Job\nclean + conform| C3

    C2 --> D1
    C3 --> D1

    D1 -->|table location → S3 raw| C2
    D1 -->|table location → S3 curated| C3

    C2 --> E3
    C3 --> E1
    C3 --> E2
```

---

## Orchestration (Airflow on MWAA)

```mermaid
flowchart TD
    T1[⏰ Daily Schedule]
    T2[S3 Sensor\nnew files in landing/]

    T1 --> J1[EMR Step\nLanding → Raw]
    T2 --> J1
    J1 --> J2[EMR Step\nRaw → Curated]
    J2 --> J3[Hive Metastore\nMSCK REPAIR TABLE]
    J3 --> N1[SNS Notification\npipeline complete]

    J1 -->|fail| A1[Airflow Alert → Slack/PagerDuty]
    J2 -->|fail| A1
```

---

## Component Map

| Component | Tool | Notes |
|-----------|------|-------|
| Object Storage | Amazon S3 | Same as managed; portable |
| CDC Ingestion | Debezium | Reads WAL from RDS/PostgreSQL → Kafka → S3 |
| SaaS Ingestion | Airbyte (self-hosted on EC2/EKS) | 300+ connectors; open-source |
| Stream Ingestion | Kafka Connect S3 Sink | Kafka → S3 in Parquet |
| Processing | Apache Spark on EMR | Auto-scaling clusters; pay per use |
| Catalog | Hive Metastore on RDS MySQL | Shared by Spark + Trino + EMR |
| Ad-hoc Query | Trino on EC2 / EKS | Federated; can query beyond S3 |
| Dashboards | Apache Superset | Open-source; connects to Trino |
| ML | SageMaker (AWS) or self-hosted MLflow | Training reads from Raw zone |
| Orchestration | Apache Airflow on MWAA | DAGs for all pipeline steps |
| Access Control | Apache Ranger on EMR | Column/row security; LDAP integration |

---

## vs 2.1 AWS Managed

| | 2.1 Managed | 2.2 OSS |
|--|-------------|---------|
| Ops overhead | Low | Medium-High |
| Cost at scale | Higher | Lower |
| Vendor lock-in | Higher (Glue/LF) | Lower (portable to Azure/GCP) |
| Query engine | Athena only | Trino (federated across sources) |
| Catalog portability | Glue-only | Hive Metastore (multi-engine) |
| Customisation | Limited | Full control |
