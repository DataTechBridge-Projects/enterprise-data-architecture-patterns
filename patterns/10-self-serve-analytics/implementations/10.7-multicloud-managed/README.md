---
layout: default
title: "10.7 — Self-Serve Analytics Engineering · Multi-Cloud Fully Managed"
---

# 10.7 — Self-Serve Analytics Engineering · Multi-Cloud Fully Managed

**Stack:** Snowflake · dbt Cloud · Tableau · AtScale / dbt Semantic Layer
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Buy (fully managed SaaS stack, cloud-portable)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources (Any Cloud)"]
        S1[Relational DBs\nPostgres · MySQL · SQL Server]
        S2[SaaS Apps\nSalesforce · NetSuite · HubSpot]
        S3[Object Storage\nS3 · ADLS · GCS]
        S4[Event Streams\nKafka · Kinesis · Event Hubs]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Fivetran\nmanaged SaaS connectors]
        I2[Snowpipe\nS3/GCS/ADLS → Snowflake]
        I3[Kafka Connector\nSnowflake Sink]
    end

    subgraph DWH["Snowflake — Storage & Compute"]
        R1[RAW database\ningested data]
        R2[TRANSFORM database\ndbt intermediate]
        R3[ANALYTICS database\nGold — facts + dims]
    end

    subgraph TRANSFORM["dbt Cloud"]
        T1[dbt models\nSQL transforms]
        T2[dbt tests\nassertions + anomaly]
        T3[dbt Semantic Layer\nMetricFlow definitions]
        T4[dbt Docs\nlineage explorer]
    end

    subgraph SEMANTIC["Semantic Layer"]
        M1[dbt Semantic Layer\nMetricFlow API]
        M2[AtScale\ncross-tool metric bus]
    end

    subgraph CONSUME["Consumption"]
        C1[Tableau Cloud\ndashboards + workbooks]
        C2[Excel / Google Sheets\nAtScale JDBC]
        C3[Looker / Power BI\nAtScale or dbt SL]
        C4[Python / Notebooks\ndirect Snowflake SQL]
        C5[Internal Apps\ndbt SL GraphQL API]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1 --> R2 --> R3
    T2 -. tests .-> R3
    T3 -. metrics .-> M1
    T4 -. docs .-> R3
    R3 -. defines .-> M2
    M1 --> C1 & C5
    M2 --> C1 & C2 & C3
    R3 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Sources (Any Cloud)"]
        A1[(RDBMS)]
        A2[SaaS APIs]
        A3[Object Storage]
        A4[Streams]
    end

    subgraph Ingestion
        B1[Fivetran\nSaaS + DB]
        B2[Snowpipe\ncloud storage]
        B3[Kafka Sink\nstreaming]
    end

    subgraph Snowflake["Snowflake Databases"]
        C1[RAW]
        C2[TRANSFORM]
        C3[ANALYTICS]
    end

    subgraph dbt["dbt Cloud"]
        D1[staging models]
        D2[intermediate models]
        D3[mart models]
        D4[MetricFlow metrics]
    end

    subgraph Semantic["Semantic Layer"]
        E1[dbt SL API\nGraphQL]
        E2[AtScale\nMDX + SQL]
    end

    subgraph Consume
        F1[Tableau Cloud]
        F2[Excel / Sheets]
        F3[Other BI tools]
        F4[Python]
    end

    A1 --> B1 --> C1
    A2 --> B1
    A3 --> B2 --> C1
    A4 --> B3 --> C1

    C1 --> D1 --> C1
    D1 -->|done| D2 --> C2
    D2 -->|done| D3 --> C3
    D3 --> D4

    D4 --> E1
    C3 --> E2

    E1 --> F1 & F4
    E2 --> F1 & F2 & F3
```

---

## Zone Design

```
Snowflake Databases
│
├── RAW
│   └── {SOURCE_SCHEMA}.{TABLE}     — exact Fivetran / Snowpipe copy
│
├── TRANSFORM
│   └── {DOMAIN}_INTERMEDIATE
│       └── INT_{ENTITY}            — joins, business logic
│
└── ANALYTICS
    ├── FINANCE
    │   └── FCT_REVENUE, DIM_ACCOUNT, DIM_DATE
    ├── MARKETING
    │   └── FCT_CAMPAIGNS, DIM_CHANNEL, DIM_AUDIENCE
    └── PRODUCT
        └── FCT_EVENTS, DIM_USER, DIM_FEATURE
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│  Snowflake RBAC + dbt SL / AtScale Role Mapping           │
│                                                           │
│  Role                Access Level    Scope                │
│  ─────────────────   ────────────    ──────────────────   │
│  DATA_ENGINEER       Read + Write    All databases        │
│  ANALYTICS_ENG       Read + Write    TRANSFORM + ANALYTICS│
│  BI_DEVELOPER        Read only       ANALYTICS only       │
│  BUSINESS_ANALYST    Metric API      dbt SL / AtScale     │
│  EXEC_VIEWER         Metric API      curated dashboards   │
│                                                           │
│  Column masking    → Snowflake Dynamic Data Masking       │
│  Row filtering     → Snowflake Row Access Policies        │
│  AtScale rows      → queryRewrite per role                │
│  Network policy    → IP allowlist per service             │
│  Secrets           → Snowflake Secrets + dbt Cloud env    │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ dbt Cloud Schedule\ndaily 03:00 UTC]
    T2[📡 Fivetran Sync Complete\nwebhook → dbt Cloud API]

    T1 --> J1[dbt run\nstaging models]
    T2 --> J1

    J1 --> J2[dbt source freshness\nfreshness assertions]
    J2 -->|pass| J3[dbt run\nintermediate models]
    J2 -->|fail| A1[Slack alert\ndbt Cloud notification]

    J3 --> J4[dbt test\nintermediate]
    J4 -->|pass| J5[dbt run\nmart models]
    J5 --> J6[dbt test\nmart assertions]
    J6 --> J7[dbt docs generate]
    J7 --> J8[AtScale cache warm\nREST API call]

    J4 -->|fail| A1
    J5 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | Snowflake | Runs on AWS / Azure / GCP; same SQL everywhere |
| Ingestion — SaaS/DB | Fivetran | 500+ connectors; normalized schemas |
| Ingestion — Files | Snowpipe | S3/ADLS/GCS → Snowflake continuous ingest |
| Ingestion — Streams | Snowflake Kafka Connector | Kafka → Snowflake Sink |
| Transformation | dbt Cloud | Snowflake adapter; CI/CD + dbt Cloud IDE |
| Metrics Definition | dbt Semantic Layer (MetricFlow) | Metric definitions in dbt YAML |
| Data Testing | dbt tests + dbt-expectations | Schema + statistical assertions |
| Lineage | dbt Cloud Explorer | Column-level lineage; impact analysis |
| Semantic Layer — dbt | dbt Semantic Layer GraphQL API | First-class Tableau + Looker integration |
| Semantic Layer — cross | AtScale | MDX/JDBC for Excel, Tableau, other BI |
| BI | Tableau Cloud | Live Snowflake or AtScale connections |
| Ad-hoc | Python + snowflake-connector-python | Direct SQL for engineers |
| Orchestration | dbt Cloud Scheduler | Job chaining; Fivetran webhook trigger |
| Secrets | Snowflake Secrets + dbt Cloud env vars | Zero plaintext credential storage |
| Monitoring | dbt Cloud + Snowflake Query History | Query performance + job failure alerts |

---

## Comparison vs 10.8 (Multi-Cloud OSS)

| Dimension | 10.7 Multi-Cloud Managed | 10.8 Multi-Cloud OSS |
|-----------|------------------------|---------------------|
| Warehouse | Snowflake (SaaS) | DuckDB / Trino (OSS) |
| dbt runtime | dbt Cloud | dbt Core + Airflow |
| Semantic layer | dbt SL + AtScale | Cube.js |
| BI | Tableau Cloud | Apache Superset |
| Infra ops | Near-zero | Moderate to high |
| Cloud portability | Native (Snowflake runs anywhere) | Fully portable |
| Cost model | Snowflake + SaaS subscriptions | Compute + OSS ops |
| Enterprise support | Snowflake + dbt + Fivetran SLAs | Community / self-support |

---

## When to Choose This Implementation

✅ Multi-cloud or cloud-agnostic mandate
✅ Snowflake already licensed and deployed
✅ Need single analytical platform across AWS + Azure + GCP
✅ Fivetran investment already in place
✅ Tableau is the enterprise standard BI tool
✅ Central data team < 10 engineers; low ops budget

❌ Snowflake cost is prohibitive → consider 10.8 (DuckDB/Trino OSS)
❌ Only on one cloud → use cloud-native 10.1, 10.3, or 10.5
❌ Real-time analytics → Pattern 7
