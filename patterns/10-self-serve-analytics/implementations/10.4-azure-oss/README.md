---
layout: default
title: "10.4 — Self-Serve Analytics Engineering · Azure OSS on Cloud"
---

# 10.4 — Self-Serve Analytics Engineering · Azure OSS on Cloud

**Stack:** Synapse Analytics / DuckDB · dbt Core · Cube.js · Apache Superset · Apache Airflow
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Build (OSS on Azure infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Azure SQL / PostgreSQL]
        S2[SaaS APIs]
        S3[ADLS Gen2\nParquet / Delta]
        S4[Event Hubs]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Airbyte on AKS\nELT connectors]
        I2[Debezium → Event Hubs\nCDC streaming]
        I3[ADF Copy Activity\nbulk loads]
    end

    subgraph DWH["Synapse / DuckDB — Storage & Compute"]
        R1[staging schema\nraw ELT data]
        R2[intermediate schema\ndbt logic]
        R3[marts schema\nGold layer]
    end

    subgraph TRANSFORM["Transformation — dbt Core + Airflow"]
        T1[dbt run\nSQL models]
        T2[dbt test + soda\ndata quality]
        T3[dbt docs site\nlineage]
    end

    subgraph SEMANTIC["Semantic Layer — Cube.js on AKS"]
        M1[Cube schemas\nJS metric definitions]
        M2[Pre-aggregations\nmaterialized rollups]
        M3[REST · GraphQL · SQL API]
    end

    subgraph CONSUME["Consumption"]
        C1[Apache Superset\ndashboards]
        C2[Any BI tool\nCube SQL API]
        C3[Internal Apps\nCube REST API]
        C4[Notebooks\ndirect SQL]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1 --> R2 --> R3
    T2 -. tests .-> R3
    T3 -. docs .-> R3
    R3 -. defines .-> M1
    M1 --> M2 --> M3
    M3 --> C1
    M3 --> C2
    M3 --> C3
    R3 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Azure SQL)]
        A2[SaaS APIs]
        A3[ADLS Gen2]
    end

    subgraph Ingestion
        B1[Airbyte\nELT]
        B2[Debezium\nCDC → Event Hubs]
    end

    subgraph DWH["Synapse SQL / DuckDB"]
        C1[staging]
        C2[intermediate]
        C3[marts]
    end

    subgraph Airflow["Airflow DAGs on AKS"]
        D1[airbyte_sync]
        D2[dbt_staging]
        D3[dbt_marts]
    end

    subgraph CubeJS["Cube.js on AKS"]
        E1[Cube schemas]
        E2[Pre-aggregations]
        E3[API endpoints]
    end

    subgraph Consume
        F1[Superset]
        F2[BI tools via SQL API]
        F3[REST apps]
    end

    A1 --> B2 --> C1
    A2 --> B1 --> C1
    A3 --> C1

    C1 --> D2 --> C1
    D2 -->|success| D3 --> C3

    C3 --> E1 --> E2 --> E3
    E3 --> F1 & F2 & F3
```

---

## Zone Design

```
Synapse Dedicated SQL Pool / DuckDB on ADLS
│
├── staging/
│   └── stg_{source}_{table}      — raw ELT, minimal casts
│
├── intermediate/
│   └── int_{domain}_{entity}     — joins, deduplication, logic
│
└── marts/
    ├── finance/
    │   └── fct_gl_entries, dim_cost_center, dim_date
    ├── customer/
    │   └── fct_interactions, dim_customer, dim_segment
    └── supply_chain/
        └── fct_inventory, dim_sku, dim_warehouse
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│  Synapse RBAC + Cube.js Role Mapping                  │
│                                                       │
│  Role               Access Level    Scope             │
│  ────────────────   ────────────    ─────────────     │
│  data-engineer      Read + Write    All schemas       │
│  analytics-eng      Read + Write    intermediate +    │
│                                     marts             │
│  bi-developer       Read only       marts only        │
│  business-user      Cube API only   defined cubes     │
│  app-service        REST API only   approved endpoints│
│                                                       │
│  Column masking   → Synapse Dynamic Data Masking      │
│  Row filtering    → Cube.js queryRewrite per role     │
│  Secrets          → Azure Key Vault + Airflow vault   │
│  Network          → AKS private cluster + VNet        │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow Schedule\ndaily 02:00 UTC]

    T1 --> J1[Airbyte operator\ntrigger sync jobs]
    J1 --> J2[Airflow sensor\nwait for Airbyte done]

    J2 --> J3[dbt BashOperator\ndbt run staging]
    J3 --> J4[dbt BashOperator\ndbt test staging]
    J4 -->|pass| J5[dbt run\nintermediate models]
    J4 -->|fail| A1[Slack webhook\nAirflow on_failure]

    J5 --> J6[dbt test\nintermediate]
    J6 -->|pass| J7[dbt run\nmart models]
    J7 --> J8[dbt docs generate\nupload to Azure Static Web App]
    J8 --> J9[Cube.js pre-agg refresh\ncurl Cube REST endpoint]

    J6 -->|fail| A1
    J7 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | Azure Synapse Dedicated SQL | DWU scaling; pause for cost savings |
| Local / Dev | DuckDB on ADLS Gen2 | Fast local SQL on Parquet for dev/test |
| Ingestion | Airbyte on AKS | 300+ connectors; Helm chart deployment |
| CDC | Debezium → Azure Event Hubs | Kafka-compatible API on Event Hubs |
| Transformation | dbt Core (CLI) | Git-versioned; run via Airflow BashOperator |
| Orchestration | Apache Airflow on AKS | KubernetesExecutor; Helm chart |
| Data Testing | dbt tests + Soda Core | Schema + SQL check assertions |
| Lineage | dbt docs (Azure Static Web App) | Auto-generated HTML served from blob |
| Semantic Layer | Cube.js on AKS | Pre-aggregations; REST + GraphQL + SQL API |
| BI | Apache Superset (AKS) | Connected via Cube SQL API |
| Secrets | Azure Key Vault + Airflow KV backend | All credentials referenced by name |
| Monitoring | Azure Monitor + Airflow callbacks | DAG failures → Slack via webhook |

---

## Comparison vs 10.3 (Azure Managed)

| Dimension | 10.4 Azure OSS | 10.3 Azure Managed |
|-----------|---------------|-------------------|
| dbt runtime | dbt Core + Airflow | dbt Cloud |
| Semantic layer | Cube.js on AKS | Power BI Dataset + AtScale |
| BI | Superset | Power BI Service |
| Microsoft integration | Partial | Native |
| Infra ops | Moderate (AKS workloads) | Near-zero |
| Cost at scale | Lower (no per-seat) | Higher (Power BI Pro × users) |
| Governance tooling | Manual / OpenMetadata | Purview native |

---

## When to Choose This Implementation

✅ Azure primary cloud but want OS flexibility
✅ Engineering team runs AKS workloads already
✅ Power BI licensing cost is prohibitive
✅ Need custom Cube.js schemas + API integrations
✅ Airbyte already deployed for other patterns

❌ Microsoft 365 / Excel self-serve is the primary use case → 10.3
❌ No AKS ops capacity → use 10.3 (managed)
❌ Real-time analytics → Pattern 7
