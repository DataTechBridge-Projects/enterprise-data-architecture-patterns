---
layout: default
title: "6.1 — Data Fabric · AWS Fully Managed"
---

# 6.1 — Data Fabric · AWS Fully Managed

**Stack:** AWS DataZone · AWS Glue Data Catalog · Lake Formation · Athena Federated Query · AWS Macie · Amazon DataZone
**Processing:** Federated Query · No Data Movement · Active Metadata
**Buy vs Build:** Buy (fully managed AWS-native fabric)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Physical Data Sources — Data Stays In Place"]
        S1[(Amazon RDS / Aurora\noperational DBs)]
        S2[(Amazon Redshift\ndata warehouse)]
        S3[S3 Data Lake\nParquet · Iceberg]
        S4[(DynamoDB\nNoSQL)]
        S5[On-prem / Other\nvia Athena connector]
    end

    subgraph META["Active Metadata Layer — AWS DataZone + Glue Catalog"]
        M1[AWS Glue Crawlers\nauto-discover schema + partitions]
        M2[Glue Data Catalog\ntechnical metadata · table registry]
        M3[AWS DataZone\nbusiness catalog · data products · subscriptions]
        M4[AWS Macie\nPII auto-classification · sensitivity tags]
    end

    subgraph GOV["Governance & Policy — Lake Formation"]
        G1[Lake Formation RBAC / ABAC\ncolumn masking · row filters · LF-Tags]
        G2[AWS IAM\nservice-level access]
        G3[CloudTrail + S3 Access Logs\naudit · lineage events]
    end

    subgraph QUERY["Federated Virtual Query — Athena"]
        Q1[Athena Federated Query\nquery RDS · DynamoDB · Redshift · S3 in-place]
        Q2[Athena + Glue Catalog\nno ETL · schema-on-read]
    end

    subgraph CONSUME["Consumers — No Data Copy"]
        F1[BI Teams\nQuickSight · Tableau via Athena]
        F2[Data Scientists\nSageMaker · Athena JDBC]
        F3[Data Engineers\nGlue ETL read if transform needed]
        F4[DataZone Portal\nself-serve discovery + subscription]
    end

    SRC -. crawl .-> M1
    M1 --> M2
    M2 --> M3
    M2 -. scan .-> M4
    M3 -. enforce policies .-> G1
    G1 --> Q1
    Q1 -. federated connector .-> S1 & S2 & S3 & S4 & S5
    Q2 --> F1 & F2 & F3
    M3 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Physical Data — In-Place"]
        A1[(RDS / Aurora)]
        A2[(Redshift)]
        A3[S3 Data Lake]
        A4[(DynamoDB)]
    end

    subgraph Metadata["Active Metadata"]
        B1[Glue Crawler\nauto schema]
        B2[Glue Data Catalog\ntechnical registry]
        B3[AWS DataZone\nbusiness catalog]
        B4[Macie\nPII tags]
    end

    subgraph Policy["Lake Formation"]
        C1[RBAC / ABAC\nLF-Tags]
        C2[Column masking\nRow filters]
    end

    subgraph Query["Athena Federated Query"]
        D1[Athena engine\nno data copy]
        D2[Lambda connectors\nRDS · DynamoDB · Redshift]
    end

    subgraph Consume
        E1[QuickSight\ndashboards]
        E2[SageMaker\nML]
        E3[DataZone Portal\nself-serve]
    end

    A1 & A2 & A3 & A4 -. crawl schema .-> B1
    B1 --> B2 --> B3
    A1 & A3 -. scan .-> B4
    B2 -. policies .-> C1 --> C2
    C2 --> D1
    D1 --> D2
    D2 -. federated read .-> A1 & A2 & A3 & A4
    D1 --> E1 & E2
    B3 --> E3
```

---

## Catalog Structure

```
AWS DataZone Domain
├── domain: finance
│   ├── data-product: customer-360
│   │   ├── asset: customer_dim         → Redshift (virtual)
│   │   └── asset: customer_events      → S3 Iceberg (virtual)
│   └── data-product: transactions
│       └── asset: transactions_raw     → S3 (virtual)
│
├── domain: operations
│   ├── data-product: inventory
│   │   └── asset: inventory_current    → DynamoDB (virtual)
│   └── data-product: orders
│       └── asset: orders_live          → RDS Aurora (virtual)
│
└── domain: marketing
    └── data-product: campaign-analytics
        └── asset: campaign_events      → S3 (virtual)

Glue Data Catalog (technical layer)
├── database: finance_raw
│   └── tables → s3://lake/finance/*/
├── database: ops_raw
│   └── tables → s3://lake/ops/*/
└── database: redshift_ext
    └── tables → Redshift Spectrum external schemas

All assets are virtual — no data physically moved to catalog.
Athena Federated Query resolves at query-time via Lambda connectors.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Lake Formation RBAC/ABAC + DataZone Subscriptions + IAM         │
│                                                                  │
│  Principal             Access Level     Scope                    │
│  ─────────────────── ─ ─────────────    ──────────────────────  │
│  data-engineer-role    SELECT + crawl   all databases            │
│  bi-analyst-role       SELECT (masked)  finance · marketing      │
│  data-scientist-role   SELECT           raw + curated assets     │
│  ml-pipeline-sa        SELECT           approved DataZone assets │
│  ops-reader-role       SELECT           ops domain only          │
│  audit-role            SELECT           CloudTrail + access logs │
│                                                                  │
│  Lake Formation → LF-Tag ABAC per domain (no per-table grants)  │
│  Column masking → PII columns via LF column-level security       │
│  Row filters    → region / team scope per analyst role           │
│  DataZone subs  → consumers subscribe; owner approves access     │
│  Macie          → auto-tag PII buckets; triggers LF policy       │
│  CloudTrail     → all Athena + Glue API calls audited            │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Scheduled Glue Crawler\nevery 6 hours]
    T2[📥 S3 Event\nnew object in lake prefix]
    T3[🔍 Macie Scheduled Scan\ndaily PII sweep]

    T1 --> C1[Glue Crawler\nupdate schema in Catalog]
    T2 --> C1
    C1 --> C2[DataZone\nsync new assets from Catalog]
    C2 --> C3[DataZone\nauto-notify subscribers of schema change]

    T3 --> M1[Macie Job\nclassify S3 objects]
    M1 --> M2[EventBridge Rule\nPII finding detected]
    M2 --> M3[Lambda\napply LF sensitive tag to column]

    C3 --> SUB[Subscriber notified\nvia DataZone portal]
    SUB --> APR[Owner approves / rejects\nsubscription request]
    APR --> ACC[IAM policy attached\nvia Lake Formation grant]

    ACC -->|access granted| Q1[Athena query\nexecuted in-place on source]
    Q1 -->|fail| ERR[CloudWatch Alarm\n→ SNS → ops team]
```

---

## Component Map

| Component | AWS Service | Notes |
|-----------|-------------|-------|
| Business Catalog | AWS DataZone | Data product marketplace; subscription-based access; domain ownership |
| Technical Catalog | AWS Glue Data Catalog | Hive-compatible; shared by Athena, EMR, Glue, Redshift Spectrum |
| Schema Discovery | AWS Glue Crawlers | Auto-detect schema from S3, RDS, Redshift; schedule or event-triggered |
| PII Detection | AWS Macie | S3 content scanning; findings trigger Lake Formation tag policies |
| Access Control | AWS Lake Formation | Column masking · row filters · LF-Tag ABAC; no IAM per-table grants |
| Federated Query | Amazon Athena + Lambda connectors | Query RDS, DynamoDB, Redshift, S3 in-place without ETL |
| S3 Connector | Athena native | Built-in Parquet, ORC, JSON, CSV, Iceberg support |
| RDS Connector | Athena JDBC Lambda | MySQL, PostgreSQL, SQL Server, Oracle via Lambda Data Source |
| DynamoDB Connector | Athena DynamoDB Lambda | Full scan or partition filter pushdown |
| Redshift Connector | Athena Redshift Lambda | In-place query without Spectrum; or use Redshift native |
| Audit & Lineage | CloudTrail + S3 Access Logs | API-level audit; DataZone lineage for data products |
| BI Access | Amazon QuickSight | Native Athena integration; SPICE or direct |
| Monitoring | CloudWatch | Athena query metrics, crawler run status, Macie findings |
| Encryption | AWS KMS | SSE-KMS on S3; TLS for all Athena federated connectors |

---

## Comparison vs 6.2 (Azure Data Fabric)

| Dimension | 6.1 AWS Managed | 6.2 Azure Managed |
|-----------|----------------|-------------------|
| Business catalog | AWS DataZone | Microsoft Purview |
| Technical catalog | Glue Data Catalog | Purview unified catalog |
| Federated query | Athena + Lambda connectors | Synapse Serverless SQL |
| Operational link | Athena RDS connector | Azure Synapse Link (Cosmos DB / SQL) |
| PII classification | AWS Macie | Purview DLP + sensitivity labels |
| Access control | Lake Formation ABAC | Azure RBAC + Purview data policy |
| Cross-cloud query | Limited (Lambda connectors) | Yes (Synapse Link + Purview cross-source) |
| Self-serve portal | DataZone marketplace | Purview data catalog |

---

## When to Choose This Implementation

✅ AWS is primary cloud and data stays on AWS
✅ Need self-serve data product marketplace (DataZone)
✅ Multiple data sources (RDS, DynamoDB, Redshift, S3) need unified virtual access
✅ Compliance requires zero data copy — Athena queries in-place
✅ Fine-grained column/row access via Lake Formation already in use

❌ Cross-cloud data sources on Azure/GCP → use 6.4 (Multi-Cloud Managed)
❌ On-prem data residency is primary constraint → use 6.6 or 6.7
❌ Full OSS stack, no vendor lock-in → use 6.5
