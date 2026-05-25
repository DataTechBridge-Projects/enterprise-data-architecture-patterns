---
layout: default
title: "7.7 — Operational Analytics · Multi-Cloud Fully Managed"
---

# 7.7 — Operational Analytics · Multi-Cloud Fully Managed

**Stack:** CockroachDB / SingleStore · Fivetran · Snowflake · Tableau · Alation
**Processing:** Streaming / Near-Real-Time · HTAP + ODS pattern
**Buy vs Build:** Buy (fully managed SaaS, cloud-agnostic)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources — Multi-Cloud"]
        S1[CockroachDB\nGlobal distributed OLTP]
        S2[SingleStore\nHTAP — mixed workload]
        S3[PostgreSQL / MySQL\nCloud or on-prem]
        S4[SaaS Apps\nSalesforce · SAP]
    end

    subgraph CDC["CDC / Sync Layer — Fivetran"]
        I1[Fivetran Connector\nCockroachDB CDC]
        I2[Fivetran Connector\nPostgres / MySQL CDC]
        I3[Fivetran Connector\nSaaS APIs]
        I4[SingleStore Replication\nread replica → Snowflake]
    end

    subgraph ODS["ODS — Snowflake"]
        O1[Schema: RAW\nraw CDC rows]
        O2[Schema: ODS\ncurrent state MERGE]
        O3[Schema: AGG\npre-computed metrics]
    end

    subgraph CATALOG["Catalog & Governance\nAlation + Snowflake RBAC"]
        C1[Alation Catalog\nlineage + stewardship]
        C2[Snowflake RBAC\nrole-based access]
        C3[Snowflake Dynamic\nData Masking]
    end

    subgraph CONSUME["Consumption"]
        F1[Tableau\nOperational Dashboards]
        F2[Snowflake SQL\nAd-hoc queries]
        F3[Snowflake Cortex\nAI-powered insights]
        F4[Snowflake Alerts\nOps notifications]
    end

    SRC --> CDC
    I1 --> O1
    I2 --> O1
    I3 --> O1
    I4 --> O1
    O1 --> O2
    O2 --> O3
    O2 & O3 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1
    C2 --> F2
    C2 --> F3
    O3 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(CockroachDB)]
        A2[(SingleStore)]
        A3[(PostgreSQL / MySQL)]
        A4[SaaS APIs]
    end

    subgraph Fivetran["Fivetran Connectors"]
        B1[CockroachDB CDC]
        B2[SingleStore Replication]
        B3[Postgres / MySQL CDC]
        B4[SaaS REST / Bulk]
    end

    subgraph Snowflake["Snowflake ODS Schemas"]
        C1[RAW schema\nfivetran-managed tables]
        C2[ODS schema\nMERGE on PK]
        C3[AGG schema\nrolling metrics]
    end

    subgraph Catalog["Alation + Snowflake RBAC"]
        D1[Alation Catalog\nlineage + glossary]
        D2[Snowflake RBAC\naccess enforcement]
    end

    subgraph Consume
        E1[Tableau\nDashboards]
        E2[Snowflake SQL\nAd-hoc]
        E3[Snowflake Cortex\nAI insights]
        E4[Snowflake Alerts\nOps notifications]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> B4 --> C1

    C1 -->|Snowflake Task\nMERGE every 5 min| C2
    C2 -->|Snowflake Task\nagg CTAS| C3

    C2 & C3 -->|catalog scan| D1 --> D2

    D2 -->|reads| E1
    D2 -->|reads| E2
    C2 -->|Cortex functions| E3
    C3 -->|threshold| E4
```

---

## Zone Design

```
Snowflake Database: OPS_ANALYTICS
│
├── Schema: RAW
│   └── {source}_{table}              -- Fivetran-managed; auto-created
│       Fivetran handles CDC + schema evolution
│
├── Schema: ODS
│   └── {domain}_{entity}             -- current state (MERGE by PK)
│       Snowflake Task: MERGE RAW → ODS every 5 min
│       Clustered by primary key + updated_at
│
└── Schema: AGG
    └── {domain}_{metric}_{grain}     -- rolling aggregates
        Snowflake Task: CTAS every 5 min
        Dynamic Tables for auto-refresh available

CockroachDB / SingleStore:
    → HTAP: analytical queries can also run directly on SingleStore read replica
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│         Snowflake RBAC + Alation + Dynamic Masking    │
│                                                       │
│  Snowflake Role         Access Level   Schema         │
│  ──────────────────     ────────────   ──────────     │
│  FIVETRAN_ROLE          Write only     RAW            │
│  OPS_ENGINEER           Read + Write   All schemas    │
│  DATA_ANALYST           Read only      ODS · AGG      │
│  BI_CONSUMER            Read only      AGG            │
│  APP_SERVICE            Read only      ODS (cols)     │
│  SYSADMIN               Admin          All            │
│                                                       │
│  Column masking → Snowflake Dynamic Data Masking      │
│  Row security   → Snowflake Row Access Policies       │
│  Encryption     → Snowflake Tri-Secret Secure (BYOK)  │
│  Network        → Private Link per cloud region       │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Fivetran\ncontinuous CDC sync]
    T2[⏰ Snowflake Task\nMERGE every 5 min]
    T3[⏰ Snowflake Task\nAGG refresh every 5 min]
    T4[⏰ Snowflake Alert\nthreshold check every 1 min]

    T1 -->|CDC rows| J1[RAW schema tables\nFiretran-managed]
    J1 --> J2[Snowflake Task\nMERGE RAW → ODS]
    T2 --> J2
    J2 --> J3[Snowflake Task\nCTAS AGG from ODS]
    T3 --> J3
    J3 --> T4

    T4 -->|threshold breach| N1[Snowflake Alert\n→ SNS / PagerDuty / Slack]

    J2 -->|fail| A1[Snowflake Task Error\n→ Snowflake Alert → PagerDuty]
    T1 -->|lag > 5 min| A2[Fivetran Monitor Alert\n→ Slack]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| OLTP Source (global) | CockroachDB | Multi-region distributed; Fivetran CDC connector |
| OLTP Source (HTAP) | SingleStore | Mixed OLTP + OLAP; direct queries possible |
| OLTP Source (standard) | PostgreSQL / MySQL | Fivetran log-based CDC |
| SaaS Sources | Salesforce, SAP, Workday | Fivetran pre-built connectors |
| CDC / Sync | Fivetran | Managed CDC; handles schema drift |
| ODS Store | Snowflake | Multi-cloud (AWS/Azure/GCP); RAW + ODS + AGG schemas |
| Merge Logic | Snowflake Tasks | MERGE on PK every 5 min |
| Aggregates | Snowflake Tasks + Dynamic Tables | CTAS or incremental Dynamic Tables |
| Catalog | Alation | Business glossary + lineage across all sources |
| Access Control | Snowflake RBAC + Row Access Policies | Role hierarchy + DDM |
| BI / Dashboards | Tableau | Live or extract connection to Snowflake |
| AI Insights | Snowflake Cortex AI | ML functions on ODS data |
| Ad-hoc SQL | Snowflake SQL Worksheets | Multi-cloud serverless |
| Alerting | Snowflake Alerts + SNS/PagerDuty | Threshold-based ops notifications |
| Monitoring | Fivetran Monitor + Snowflake Query History | Sync lag + query performance |

---

## Comparison vs 7.8 (Hybrid OSS Self-Hosted)

| Dimension | 7.7 Multi-Cloud Managed | 7.8 Hybrid OSS Self-Hosted |
|-----------|------------------------|---------------------------|
| CDC Engine | Fivetran (managed) | Debezium (self-operated) |
| Message Bus | None (Fivetran direct) | Kafka (self-hosted) |
| Stream Processor | Snowflake Tasks | Apache Pinot / Druid |
| ODS Store | Snowflake | Apache Pinot / Druid |
| Latency | ~5 min (Fivetran) | ~1–5 s (Pinot/Druid) |
| Query Latency | Seconds (Snowflake) | Sub-second (Pinot/Druid) |
| Ops Overhead | Low | Very High |
| Cost Model | SaaS subscription | Infrastructure + ops |
| Open Format | No (Snowflake) | Yes (Parquet/native) |

---

## When to Choose This Implementation

✅ Multi-cloud environment (AWS + Azure + GCP sources)  
✅ CockroachDB or SingleStore already deployed for OLTP  
✅ Snowflake is the analytical standard across the org  
✅ Fivetran already used for other pipelines  
✅ 5-minute latency is acceptable for operational analytics  

❌ Need sub-second query latency → use 7.8 (Pinot/Druid)  
❌ Need sub-minute data freshness → use 7.8 (Kafka + Pinot)  
❌ Open source / no vendor lock-in required → use 7.8
