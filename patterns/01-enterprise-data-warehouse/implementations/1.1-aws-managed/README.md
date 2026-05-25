---
layout: default
title: "1.1 — Enterprise Data Warehouse · AWS Fully Managed"
---

# 1.1 — Enterprise Data Warehouse · AWS Fully Managed

**Stack:** Redshift · AWS Glue · dbt Cloud · QuickSight · Tableau
**Processing:** Batch-first · Schema-on-Write
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[RDS / Aurora\nOLTP Systems]
        S2[SaaS Apps\nSalesforce · SAP · Workday]
        S3[Files\nCSV · JSON · Parquet]
        S4[Clickstream / Events]
        S5[Third-Party Data Feeds]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[AWS DMS\nDB CDC + Full Load]
        I2[AWS Glue ETL\nSaaS / File Connectors]
        I3[Kinesis Firehose\nEvent Streams]
    end

    subgraph STAGING["Staging — S3 + Redshift Staging Schema"]
        ST1[S3 Landing Bucket\nraw files · short TTL]
        ST2[Redshift STG Schema\ntype-cast · de-duped]
    end

    subgraph EDW["EDW — Amazon Redshift"]
        E1[Core / Integration Layer\n3NF or Data Vault Hubs/Links/Sats]
        E2[Dimensional Layer\nKimball Star Schemas]
        E3[Data Marts\nFinance · Sales · HR · Ops]
    end

    subgraph CATALOG["Catalog & Governance\nGlue Data Catalog + Lake Formation"]
        C1[Glue Crawler\nauto-schema on S3 staging]
        C2[Lake Formation\ncolumn/row security]
        C3[AWS Macie\nPII detection]
    end

    subgraph CONSUME["Consumption"]
        F1[Amazon QuickSight\nSelf-service dashboards]
        F2[Tableau / Power BI\nEnterprise BI]
        F3[Redshift Query Editor\nAd-hoc SQL]
        F4[SageMaker\nML on Redshift data]
    end

    SRC --> INGEST
    INGEST --> ST1 --> ST2
    ST2 --> E1 --> E2 --> E3
    ST1 -. register .-> C1
    C1 -. enforce .-> C2
    E3 --> F1
    E3 --> F2
    E2 --> F3
    E2 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[SaaS APIs\nSalesforce · SAP]
        A3[Files / Uploads]
        A4[Event Streams]
    end

    subgraph Ingestion
        B1[AWS DMS\nCDC / Full Load]
        B2[Glue ETL Jobs\nSaaS Connectors]
        B3[Kinesis Firehose\nStream Buffer]
    end

    subgraph Staging["Staging"]
        C1[S3 Landing\ns3://edw-landing/]
        C2[Redshift STG\nstg_* schemas]
    end

    subgraph EDW_Layers["EDW — Amazon Redshift"]
        D1[Integration Layer\ncore_* schemas]
        D2[Dimensional Layer\ndim_* / fact_* tables]
        D3[Data Marts\nmart_finance · mart_sales]
    end

    subgraph Catalog["Glue Catalog + Lake Formation"]
        E1[Glue Crawler\nAuto Schema]
        E2[Lake Formation\nAccess Control]
    end

    subgraph Consume
        F1[QuickSight\nDashboards]
        F2[Tableau\nEnterprise BI]
        F3[Ad-hoc SQL\nQuery Editor]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> C1
    A4 --> B3 --> C1

    C1 -->|COPY command\nbulk load| C2
    C2 -->|dbt models\ntransform + conform| D1
    D1 -->|dbt models\nstar schema build| D2
    D2 -->|dbt models\nbusiness aggregations| D3

    C1 -->|Crawler\nauto schema| E1
    E1 --> E2

    D3 --> F1
    D3 --> F2
    D2 --> F3
```

---

## Zone Design

```
Redshift Database: edw_prod
│
├── stg_{source}/             -- Staging schemas (raw ingest, minimal transform)
│   ├── stg_salesforce__accounts
│   ├── stg_rds__orders
│   └── stg_sap__gl_entries
│
├── core/                     -- Integration layer (3NF or Data Vault)
│   ├── hub_customer
│   ├── hub_product
│   ├── lnk_order_customer
│   └── sat_customer_details
│
├── mart_finance/             -- Finance data mart
│   ├── dim_date
│   ├── dim_cost_center
│   └── fact_gl_transactions
│
├── mart_sales/               -- Sales data mart
│   ├── dim_customer
│   ├── dim_product
│   └── fact_orders
│
└── mart_hr/                  -- HR data mart
    ├── dim_employee
    └── fact_headcount

S3: s3://<company>-edw-landing/
└── {source}/{table}/year=YYYY/month=MM/day=DD/
    └── raw files as-received (CSV, JSON, Parquet)
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│         Redshift + Lake Formation Access Control          │
│                                                           │
│  IAM / DB Role      Access Level        Schema Scope      │
│  ────────────────   ────────────        ─────────────     │
│  data-engineer      Read + Write        All schemas       │
│  dbt-runner         Read + Write        stg + core + mart │
│  bi-analyst         Read only           mart_* only       │
│  finance-analyst    Read only           mart_finance       │
│  sales-analyst      Read only           mart_sales         │
│  data-scientist     Read only           core + mart_*      │
│  redshift-admin     Full admin          All               │
│                                                           │
│  Column masking  → PII via Lake Formation (S3 layer)      │
│  Row filtering   → Redshift RLS policies per role         │
│  Encryption      → AWS KMS (SSE-KMS) + Redshift TDE       │
│  AWS Macie       → PII auto-tag on S3 landing             │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Schedule Trigger\ndbt Cloud Job · nightly 01:00 UTC]
    T2[📡 Event Trigger\nS3 PutObject on landing/]

    T1 --> J1[AWS Glue ETL\nSource → S3 Landing]
    T2 --> J2[Redshift COPY\nS3 Landing → STG schemas]

    J2 --> J3[dbt Cloud Job\nSTG → Core layer\nconform + deduplicate]
    J3 --> J4[dbt Cloud Job\nCore → Dimensional layer\nstar schema build]
    J4 --> J5[dbt Cloud Job\nDimensional → Data Marts\nbusiness aggregations]
    J5 --> J6[dbt Tests\ndata quality assertions]
    J6 --> N1[SNS Alert\njob complete · mart refreshed]

    J1 -->|fail| A1[CloudWatch Alarm\n→ SNS → PagerDuty]
    J5 -->|fail| A1
    J6 -->|test fail| A2[dbt Cloud Alert\n→ Slack]
```

---

## Component Map

| Component | AWS Service / Tool | Notes |
|-----------|-------------------|-------|
| Data Warehouse | Amazon Redshift | RA3 nodes with managed storage; auto WLM |
| DB Ingestion | AWS DMS | Full load + CDC from RDS/Oracle/SQL Server |
| SaaS Ingestion | AWS Glue ETL | Built-in Salesforce/SAP connectors |
| Stream Ingestion | Kinesis Firehose | Buffers events → S3 landing in Parquet |
| Staging Storage | Amazon S3 | COPY into Redshift; lifecycle to Glacier |
| Schema Registry | AWS Glue Data Catalog | Hive-compatible; crawls S3 landing |
| Access Control | Lake Formation + Redshift RLS | Column/row security at catalog + DB layers |
| PII Detection | AWS Macie | Auto-classify S3 objects before COPY |
| Transformation | dbt Cloud | Staging → Core → Dimensional → Marts |
| Orchestration | dbt Cloud Jobs + Airflow (MWAA) | dbt native for dbt jobs; MWAA for complex DAGs |
| BI / Dashboards | Amazon QuickSight | SPICE cache + direct Redshift query |
| Enterprise BI | Tableau / Power BI | Direct Redshift connection |
| Encryption | AWS KMS | SSE-KMS on S3 + Redshift TDE |
| Monitoring | CloudWatch + CloudTrail + dbt Cloud | Job metrics + query audit + dbt test results |

---

## Comparison vs 1.2 (AWS OSS)

| Dimension | 1.1 AWS Managed (Buy) | 1.2 AWS OSS (Build) |
|-----------|----------------------|---------------------|
| Warehouse | Redshift (managed) | Redshift + Airbyte OSS connectors |
| Ingestion | AWS DMS + Glue (managed) | Airbyte (self-hosted on EKS) |
| Transform | dbt Cloud (managed) | dbt Core + Airflow (self-managed) |
| Orchestration | dbt Cloud Jobs | Apache Airflow on MWAA |
| BI | QuickSight + Tableau | Apache Superset (self-hosted) |
| Ops burden | Low — AWS manages compute | Medium — manage Airbyte + Airflow |
| Cost model | Higher per-unit, lower ops | Lower per-unit, higher ops cost |
| Connector breadth | AWS ecosystem-first | 300+ Airbyte connectors |

---

## When to Choose This Implementation

✅ AWS is primary cloud
✅ Want zero infrastructure management
✅ Structured reporting and BI are primary workloads
✅ Finance, HR, Sales analytics with strict governance
✅ dbt Cloud manages all transformation logic

❌ Need schema-on-read flexibility → use Pattern 2 (Data Lake)
❌ Need ACID on large-scale unstructured data → use Pattern 3 (Lakehouse)
❌ Sub-second latency required → use Pattern 4 (Streaming)
❌ Budget-constrained; prefer OSS → use 1.2 (AWS OSS)
