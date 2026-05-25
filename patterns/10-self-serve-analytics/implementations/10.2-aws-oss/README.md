---
layout: default
title: "10.2 — Self-Serve Analytics Engineering · AWS OSS on Cloud"
---

# 10.2 — Self-Serve Analytics Engineering · AWS OSS on Cloud

**Stack:** Redshift / Trino · dbt Core · Cube.js · Apache Superset · Apache Airflow
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Build (OSS on cloud infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[RDS / Aurora]
        S2[SaaS APIs]
        S3[S3 Data Lake]
        S4[Kinesis Streams]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Airbyte\nSaaS + DB connectors]
        I2[AWS DMS\nCDC replication]
        I3[Kinesis Firehose\nstream → S3]
    end

    subgraph DWH["Redshift / Trino — Storage & Compute"]
        R1[staging schema\nraw ingested data]
        R2[intermediate schema\ndbt core models]
        R3[marts schema\nGold — facts + dims]
    end

    subgraph TRANSFORM["Transformation — dbt Core + Airflow"]
        T1[dbt run\nSQL transforms]
        T2[dbt test\ndata quality]
        T3[dbt docs\nlineage site]
    end

    subgraph SEMANTIC["Semantic Layer — Cube.js"]
        M1[Cube schemas\nJS metric definitions]
        M2[Pre-aggregations\ncached rollups]
        M3[API layer\nREST · GraphQL · SQL]
    end

    subgraph CONSUME["Consumption"]
        C1[Apache Superset\ndashboards]
        C2[Any BI tool\nvia Cube SQL API]
        C3[Internal Apps\nCube REST API]
        C4[Jupyter\ndirect Redshift SQL]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1 --> R2 --> R3
    T2 -. tests .-> R2 & R3
    T3 -. docs .-> R3
    R3 -. defines .-> M1
    M1 --> M2
    M3 -. exposes .-> M1
    M2 --> C1
    M3 --> C2
    M3 --> C3
    R3 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[SaaS APIs]
        A3[S3 / Kinesis]
    end

    subgraph Ingestion
        B1[Airbyte\nELT connectors]
        B2[AWS DMS\nCDC]
    end

    subgraph Redshift["Redshift Schemas"]
        C1[staging]
        C2[intermediate]
        C3[marts]
    end

    subgraph dbt["dbt Core via Airflow"]
        D1[DAG: dbt_staging]
        D2[DAG: dbt_core]
        D3[DAG: dbt_marts]
    end

    subgraph CubeJS["Cube.js — Semantic Layer"]
        E1[Cube Schemas\nmetric + dimension JS]
        E2[Pre-aggregations\nRedshift materialized]
        E3[REST / GraphQL / SQL API]
    end

    subgraph Consume
        F1[Superset\ndashboards]
        F2[BI tools\nSQL API]
        F3[Apps\nREST API]
    end

    A1 --> B2 --> C1
    A2 --> B1 --> C1
    A3 --> C1

    C1 --> D1 --> C1
    D1 -->|success| D2 --> C2
    D2 -->|success| D3 --> C3

    C3 -->|schema file| E1
    E1 --> E2 --> E3

    E3 --> F1
    E3 --> F2
    E3 --> F3
```

---

## Zone Design

```
Redshift Schemas
│
├── staging/
│   └── stg_{source}_{table}       — raw ELT copy, type casts only
│
├── intermediate/
│   └── int_{domain}_{entity}      — joins, business logic, dedupe
│
└── marts/
    ├── finance/
    │   └── fct_revenue, dim_account, dim_date
    ├── growth/
    │   └── fct_signups, dim_campaign
    └── operations/
        └── fct_orders, dim_product, dim_region
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│  Redshift RBAC + Cube.js Role Mapping                 │
│                                                       │
│  Role               Access Level    Scope             │
│  ────────────────   ────────────    ─────────────     │
│  data-engineer      Read + Write    All schemas       │
│  analytics-eng      Read + Write    intermediate +    │
│                                     marts             │
│  bi-developer       Read only       marts only        │
│  business-user      Cube API only   defined cubes     │
│  app-service        REST API        approved endpoints│
│                                                       │
│  Column masking   → Redshift column-level grants      │
│  Row security     → Cube.js queryRewrite filters      │
│  Secret mgmt      → AWS Secrets Manager              │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow Schedule\ndaily 02:00 UTC]
    T2[📡 Airbyte Sync Done\nAirflow sensor]

    T1 --> J1[Airbyte trigger\nsync all sources]
    T2 --> J2[dbt run\nstaging models]

    J1 --> T2

    J2 --> J3[dbt test\nsource freshness]
    J3 -->|pass| J4[dbt run\nintermediate models]
    J3 -->|fail| A1[Slack alert\nAirflow callback]

    J4 --> J5[dbt test\nintermediate assertions]
    J5 -->|pass| J6[dbt run\nmart models]
    J6 --> J7[dbt docs generate\npush to S3 static site]
    J7 --> J8[Cube.js cache refresh\nwarm pre-aggregations]

    J5 -->|fail| A1
    J6 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | Amazon Redshift | RA3 nodes; or Trino over S3 for pure ELT |
| Ingestion | Airbyte (self-hosted on ECS) | 300+ connectors; custom via CDK |
| CDC | AWS DMS | RDS/Aurora → Redshift staging |
| Transformation | dbt Core | CLI-based; version-controlled in Git |
| Orchestration | Apache Airflow (MWAA or ECS) | DAG per dbt selector |
| Data Testing | dbt tests + dbt-expectations | Schema + custom SQL assertions |
| Lineage | dbt docs (S3 static site) | Auto-generated HTML lineage graph |
| Semantic Layer | Cube.js (ECS / EC2) | Pre-aggregations; REST + GraphQL + SQL APIs |
| BI | Apache Superset | Connected to Cube SQL API endpoint |
| Ad-hoc | Jupyter + psycopg2 | Direct Redshift for power users |
| Secrets | AWS Secrets Manager | Injected into Airflow + dbt env vars |
| Monitoring | CloudWatch + Airflow alerts | DAG failure → SNS → Slack |

---

## Comparison vs 10.1 (AWS Managed)

| Dimension | 10.2 AWS OSS | 10.1 AWS Managed |
|-----------|-------------|-----------------|
| dbt runtime | dbt Core + Airflow | dbt Cloud (hosted) |
| Semantic layer | Cube.js (self-hosted) | AtScale SaaS |
| BI tool | Apache Superset | Tableau Cloud |
| Infra ops | Moderate (Airflow + Cube.js) | Near-zero |
| Cost at scale | Lower (no per-seat SaaS) | Higher (SaaS subscriptions) |
| Customization | High (code everything) | Limited to SaaS features |
| Time to first metric | Longer setup | Days with connectors |

---

## When to Choose This Implementation

✅ AWS primary cloud, want OSS flexibility
✅ Engineering team comfortable managing Airflow + Cube.js
✅ Per-seat SaaS costs are prohibitive at scale
✅ Need deep customization of semantic layer schemas
✅ Existing Airbyte investment

❌ Small team with no infra ops capacity → use 10.1 (AWS Managed)
❌ Exec mandated Tableau / Power BI → add those atop Cube SQL API
❌ Real-time requirements → Pattern 7
