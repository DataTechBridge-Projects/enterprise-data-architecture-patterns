---
layout: default
title: "7.1 — Operational Analytics · AWS Fully Managed"
---

# 7.1 — Operational Analytics · AWS Fully Managed

**Stack:** Aurora · AWS DMS (CDC) · Redshift · QuickSight · Amazon EventBridge
**Processing:** Streaming / Near-Real-Time · ODS pattern
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources"]
        S1[Aurora PostgreSQL\nOrders · Inventory]
        S2[RDS MySQL\nCRM · ERP]
        S3[DynamoDB\nSession · Events]
        S4[API Gateway\nApp Events]
    end

    subgraph CDC["CDC / Ingestion Layer"]
        I1[AWS DMS\nCDC Full Load + Ongoing]
        I2[DynamoDB Streams\n→ Lambda → Kinesis]
        I3[Kinesis Data Streams\nApp event capture]
    end

    subgraph ODS["ODS — Amazon Redshift"]
        O1[Staging Schema\nraw CDC rows]
        O2[ODS Schema\nmerged current state]
        O3[Aggregates Schema\npre-computed metrics]
    end

    subgraph CATALOG["Catalog & Governance\nAWS Glue Data Catalog + Lake Formation"]
        C1[Glue Catalog\ntable schemas]
        C2[Lake Formation\nRBAC / column masking]
    end

    subgraph CONSUME["Consumption"]
        F1[QuickSight\nOperational Dashboards]
        F2[Amazon Athena\nAd-hoc SQL]
        F3[Redshift API\nApp-facing queries]
        F4[EventBridge\nAlerts → Ops teams]
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
        A1[(Aurora PostgreSQL)]
        A2[(RDS MySQL)]
        A3[(DynamoDB)]
        A4[API Gateway Events]
    end

    subgraph Ingestion
        B1[AWS DMS\nCDC → Redshift]
        B2[DynamoDB Streams\n+ Lambda]
        B3[Kinesis Data Streams\nFirehose buffer]
    end

    subgraph ODS["Redshift ODS Schemas"]
        C1[staging\nraw CDC rows]
        C2[ods\nmerged current state]
        C3[agg\npre-computed metrics]
    end

    subgraph Catalog["Glue Catalog + Lake Formation"]
        D1[Glue Catalog\nSchema Registry]
        D2[Lake Formation\nAccess Control]
    end

    subgraph Consume
        E1[QuickSight\nDashboards]
        E2[Athena\nAd-hoc SQL]
        E3[App API\nRedshift Data API]
        E4[EventBridge\nOps Alerts]
    end

    A1 --> B1 --> C1
    A2 --> B1
    A3 --> B2 --> C1
    A4 --> B3 --> C1

    C1 -->|DMS task\nupsert by PK| C2
    C2 -->|Redshift Scheduled Query\n5-min refresh| C3

    C2 & C3 -->|auto schema| D1
    D1 --> D2

    D2 -->|reads via catalog| E1
    D2 -->|reads via catalog| E2
    C2 -->|Redshift Data API| E3
    C3 -->|threshold check| E4
```

---

## Zone Design

```
Redshift cluster: ops-analytics
│
├── staging (schema)
│   └── {source}_{table}          -- raw CDC rows, full + delta
│
├── ods (schema)
│   └── {domain}_{entity}         -- merged current state (UPSERT by PK)
│       Refresh: DMS ongoing CDC + stored procedure merge every 60s
│
└── agg (schema)
    └── {domain}_{metric}_{grain} -- pre-rolled hourly / 5-min aggregates
        Refresh: Redshift Scheduled Query every 5 min
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│          Lake Formation + Redshift RBAC               │
│                                                       │
│  IAM Role / DB User    Access Level    Schema         │
│  ──────────────────    ────────────    ──────────     │
│  dms-replication       Write only      staging        │
│  ops-engineer          Read + Write    All schemas    │
│  ops-analyst           Read only       ods · agg      │
│  app-service-account   Read only       ods (select cols) │
│  bi-consumer           Read only       agg            │
│  eventbridge-role      Invoke only     Lambda/alerts  │
│                                                       │
│  Column masking → PII (Lake Formation dynamic mask)   │
│  Row security   → region/team via Redshift RLS        │
│  Encryption     → KMS SSE on S3 + Redshift at-rest   │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ DMS Ongoing CDC\ncontinuous replication]
    T2[⏰ Redshift Scheduled Query\nevery 5 min]
    T3[⏰ EventBridge Rule\nevery 1 min]

    T1 --> J1[Staging → ODS Merge\nRedshift Stored Proc\nupsert by primary key]
    T2 --> J2[ODS → Agg Refresh\nwindow aggregates\ninsert overwrite]
    J2 --> J3[Threshold Check\nRedshift ML or SQL]
    J3 -->|breach| N1[EventBridge → SNS\n→ PagerDuty / Slack]

    T3 --> J4[DMS Task Health Check\nvia CloudWatch metric]
    J4 -->|lag > 60s| A1[CloudWatch Alarm\n→ SNS → on-call]

    J1 -->|fail| A1
    J2 -->|fail| A1
```

---

## Component Map

| Component | AWS Service | Notes |
|-----------|-------------|-------|
| OLTP Source | Aurora PostgreSQL / RDS MySQL | Binlog CDC enabled |
| Event Source | DynamoDB Streams | Change stream to Kinesis |
| App Events | Kinesis Data Streams + Firehose | Buffered to S3 or Redshift |
| CDC Engine | AWS DMS | Full load + ongoing; Redshift target with UPSERT |
| ODS Store | Amazon Redshift | Staging + ODS + Agg schemas; ra3 nodes |
| Merge Logic | Redshift Stored Procedures | PK-based merge every 60 s |
| Pre-aggregation | Redshift Scheduled Queries | 5-min grain; materialized views |
| Catalog | AWS Glue Data Catalog | Shared with Athena |
| Access Control | Lake Formation + Redshift RBAC | Column masking + RLS |
| BI / Dashboards | Amazon QuickSight (SPICE) | SPICE refresh every 15 min |
| Ad-hoc SQL | Amazon Athena | Federated query into Redshift ODS |
| App-facing API | Redshift Data API | Serverless HTTP query for applications |
| Alerting | EventBridge + SNS | Threshold breach → ops teams |
| Monitoring | CloudWatch + DMS metrics | Replication lag, latency SLAs |
| Encryption | AWS KMS | SSE-KMS on all storage |

---

## Comparison vs 7.2 (AWS OSS)

| Dimension | 7.1 AWS Fully Managed | 7.2 AWS OSS on Cloud |
|-----------|----------------------|----------------------|
| CDC Engine | AWS DMS | Debezium on EC2/ECS |
| Message Bus | None (DMS direct) | Kafka (MSK) |
| Stream Processor | DMS + Redshift SP | Apache Flink |
| ODS Store | Redshift | Apache Iceberg on S3 |
| Latency | ~60 s (DMS+merge) | ~5–15 s (Flink) |
| Ops Overhead | Low | High |
| Cost Model | Instance + DPU-hour | EC2 + MSK + EMR |
| SQL Flexibility | Redshift SQL | Flink SQL + Trino |
| Open Format | No | Yes (Iceberg) |

---

## When to Choose This Implementation

✅ AWS is primary cloud  
✅ Existing Aurora / RDS sources with binlog enabled  
✅ Sub-minute ODS latency is sufficient  
✅ BI team uses QuickSight  
✅ Want zero Kafka/Flink expertise overhead  

❌ Need sub-5-second latency → use 7.2 (Flink)  
❌ Need open table format (vendor-neutral) → use 7.2 (Iceberg)  
❌ Sources are non-AWS databases → use 7.8 (Debezium hybrid)
