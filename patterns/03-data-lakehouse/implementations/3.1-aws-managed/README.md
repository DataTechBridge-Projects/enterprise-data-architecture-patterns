---
layout: default
title: "3.1 — Data Lakehouse · AWS Fully Managed"
---

# 3.1 — Data Lakehouse · AWS Fully Managed

**Stack:** S3 · Apache Iceberg · AWS Glue · Lake Formation · Redshift Spectrum · dbt Cloud
**Processing:** Batch-first · ACID Transactions · Schema Evolution · Medallion Architecture
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[RDS / Aurora]
        S2[SaaS Apps\nSalesforce · SAP]
        S3[Files\nCSV · JSON · Parquet]
        S4[Clickstream / Logs]
        S5[IoT / Kinesis Events]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[AWS DMS\nDB → Iceberg Bronze CDC]
        I2[AWS Glue ETL\nSaaS / Files → Bronze]
        I3[Kinesis Firehose\n→ S3 Bronze]
    end

    subgraph STORAGE["Storage — S3 · Apache Iceberg"]
        Z1[BRONZE\ns3://bronze/\nRaw ingest · ACID]
        Z2[SILVER\ns3://silver/\ndbt cleanse · MERGE dedup]
        Z3[GOLD\ns3://gold/\ndbt aggregate · Kimball dims]
    end

    subgraph CATALOG["Catalog & Governance\nGlue Data Catalog + Lake Formation"]
        C1[Glue Catalog\nIceberg REST · snapshots]
        C2[Lake Formation\ncolumn masking · row filters]
        C3[AWS Macie\nPII classification]
    end

    subgraph CONSUME["Consumption"]
        F1[Amazon Athena\nad-hoc SQL · Iceberg native]
        F2[Redshift Spectrum\nBI · Power BI · QuickSight]
        F3[SageMaker\nML training · Silver Iceberg]
    end

    SRC --> INGEST
    INGEST --> Z1 --> Z2 --> Z3
    Z1 & Z2 & Z3 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1 & F2
    Z2 --> F3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[SaaS APIs\nSalesforce · SAP]
        A3[Files / S3 Drop]
        A4[Kinesis Stream]
    end

    subgraph Ingestion
        B1[AWS DMS\nCDC Full Load]
        B2[AWS Glue ETL\nIceberg writer]
        B3[Kinesis Firehose\n→ S3 Bronze]
    end

    subgraph S3_Iceberg["S3 — Iceberg Medallion Zones"]
        C1[🥉 Bronze\ns3://bronze/\nmetadata + data]
        C2[🥈 Silver\ns3://silver/\nmetadata + data]
        C3[🥇 Gold\ns3://gold/\nmetadata + data]
    end

    subgraph Catalog["Glue Catalog · Lake Formation"]
        D1[Iceberg REST Catalog\nGlue endpoint]
        D2[Lake Formation\nACL · Column Mask]
    end

    subgraph Consume
        E1[Amazon Athena\nServerless SQL]
        E2[Redshift Spectrum\nExternal Tables]
        E3[SageMaker\nML Training]
        E4[QuickSight\nDashboards]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B2
    A4 --> B3 --> C1

    C1 -->|dbt Cloud\nGlue Job MERGE| C2
    C2 -->|dbt Cloud\nGlue Job MERGE| C3

    C1 -.->|register snapshot| D1
    C2 -.->|register snapshot| D1
    C3 -.->|register snapshot| D1
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
│       │   ├── 00000-*.metadata.json   ← Iceberg table metadata
│       │   └── snap-*.avro             ← manifest list
│       └── data/
│           └── {year}/{month}/{day}/
│               └── *.parquet
│
├── silver/
│   └── {domain}/{entity}/
│       ├── metadata/
│       └── data/
│           └── {hidden-partition}/
│               └── *.parquet           ← cleansed, deduped, conformed
│
└── gold/
    └── {domain}/{model}/
        ├── metadata/
        └── data/
            └── *.parquet               ← dbt-built dims, facts, aggregates
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│         Lake Formation · IAM · KMS                        │
│                                                           │
│  IAM Role            Access Level      Zone(s)           │
│  ──────────────────  ──────────────    ─────────────     │
│  dms-ingest-role     Write only        Bronze            │
│  glue-etl-role       Read + Write      Bronze → Silver   │
│  dbt-cloud-role      Read + Write      Silver → Gold     │
│  data-engineer       Read + Write      All zones         │
│  data-analyst        Read only         Gold only         │
│  data-scientist      Read only         Silver + Gold     │
│  redshift-spectrum   Read only         Gold only         │
│  sagemaker-role      Read only         Silver + Gold     │
│                                                           │
│  Column masking  → PII fields via Lake Formation         │
│  Row filtering   → region / team LF-Tag conditions       │
│  KMS per zone    → separate CMKs for Bronze/Silver/Gold  │
│  Macie scanning  → auto-tag PII on Bronze arrival        │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ dbt Cloud Schedule\ndaily 02:00 UTC]
    T2[📡 S3 Event\nPutObject on bronze/]

    T1 --> J1[AWS Glue Job\nSource → Bronze\nIceberg MERGE / APPEND]
    T2 --> J2[dbt Cloud Job\nbronze → silver models\ncleanse + dedup]

    J1 --> J2
    J2 --> J3[dbt Cloud Job\nsilver → gold models\ndims + facts + aggregates]
    J3 --> J4[Iceberg Maintenance\nGlue Job: expire snapshots\n+ rewrite data files]
    J4 --> N1[SNS Notification\npipeline complete]

    J1 -->|fail| A1[CloudWatch Alarm\n→ SNS → PagerDuty]
    J2 -->|fail| A1
    J3 -->|fail| A1

    J3 --> DQ1[dbt Tests\nnot_null · unique\nreferential integrity]
    DQ1 -->|fail| A1
```

---

## Component Map

| Component | AWS Service / Tool | Notes |
|-----------|-------------------|-------|
| Object Storage | Amazon S3 | Iceberg metadata + Parquet data files |
| Table Format | Apache Iceberg | ACID, time travel, hidden partitioning, V2 row deletes |
| DB Ingestion | AWS DMS | Full load + CDC; targets Iceberg Bronze via Glue |
| SaaS / File Ingestion | AWS Glue ETL | Spark-based Iceberg writer; built-in connectors |
| Stream Ingestion | Kinesis Firehose | Buffers stream → S3; Glue job converts to Iceberg |
| Schema Catalog | AWS Glue Data Catalog | Iceberg REST catalog endpoint for Spark/Athena |
| Access Control | AWS Lake Formation | Column masking, row filters, LF-Tag ABAC |
| PII Detection | AWS Macie | Auto-classify S3 objects on Bronze |
| Transform / dbt | dbt Cloud | Bronze→Silver→Gold models; tests + docs |
| Glue Processing | AWS Glue Jobs (Spark) | MERGE INTO Iceberg tables for ingestion |
| Ad-hoc Query | Amazon Athena v3 | Native Iceberg support, serverless |
| BI Query Engine | Redshift Spectrum | External Iceberg tables via Glue Catalog |
| ML Consumption | Amazon SageMaker | Reads Silver/Gold via Athena or Glue |
| Dashboards | Amazon QuickSight | Connects via Athena or Redshift Spectrum |
| Orchestration | dbt Cloud Scheduler | Triggers Glue jobs + dbt runs |
| Table Maintenance | AWS Glue Job | `expire_snapshots`, `rewrite_data_files` via Iceberg API |
| Encryption | AWS KMS | SSE-KMS; separate CMK per zone |
| Monitoring | CloudWatch + CloudTrail | Glue job metrics + S3 data access audit |

---

## Comparison vs 2.1 — Data Lake · AWS Fully Managed

| Dimension | 2.1 Data Lake (AWS Managed) | 3.1 Lakehouse (AWS Managed) |
|-----------|-----------------------------|-----------------------------|
| Table Format | Raw Parquet (no format) | Apache Iceberg (ACID log) |
| ACID Transactions | ❌ | ✅ MERGE / UPDATE / DELETE |
| Time Travel | ❌ | ✅ Query any prior snapshot |
| Schema Enforcement | Schema-on-read only | Schema-on-write + evolution |
| CDC Pattern | Full partition overwrite | Iceberg MERGE INTO |
| Row-Level Deletes | Partition rewrite | Iceberg V2 positional deletes |
| dbt Integration | Glue jobs only | dbt Cloud models on Iceberg |
| GDPR Erasure | Partition drop (coarse) | Row-level delete without full rewrite |
| Query Engine | Athena (plain Parquet) | Athena v3 (native Iceberg) |
| BI Layer | Redshift Spectrum (raw) | Redshift Spectrum (Gold Iceberg tables) |
| Small File Problem | Manual Glue compaction | Iceberg `rewrite_data_files` |
| Operational Overhead | Low | Medium (snapshot expiry + compaction) |
| Best For | Exploration, append-only | OLAP + mutable CDC + BI serving |
