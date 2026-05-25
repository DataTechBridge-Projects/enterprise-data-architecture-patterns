---
layout: default
title: "10.8 — Self-Serve Analytics Engineering · Multi-Cloud OSS Portable"
---

# 10.8 — Self-Serve Analytics Engineering · Multi-Cloud OSS Portable

**Stack:** DuckDB / Trino · dbt Core · Cube.js · Apache Superset · Apache Airflow
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Build (fully OSS, runs on any cloud)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources (Any Cloud)"]
        S1[RDBMS\nPostgres · MySQL]
        S2[SaaS APIs]
        S3[Object Storage\nS3 · ADLS · GCS]
        S4[Kafka Streams]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Airbyte\nself-hosted ELT]
        I2[Debezium\nCDC → Kafka]
        I3[Kafka → Object Storage\nFlink / direct sink]
    end

    subgraph STORAGE["Storage Layer — Object Storage + Iceberg"]
        Z1[raw/\nParquet on S3/ADLS/GCS]
        Z2[intermediate/\ndbt models]
        Z3[marts/\nGold layer — facts + dims]
    end

    subgraph QUERY["Query Engine — Trino / DuckDB"]
        Q1[Trino\nfederated SQL over Iceberg]
        Q2[DuckDB\nlocal / dev / small datasets]
    end

    subgraph TRANSFORM["dbt Core + Airflow"]
        T1[dbt run\nSQL models]
        T2[dbt test\nassertions]
        T3[dbt docs\nlineage]
    end

    subgraph SEMANTIC["Cube.js — Semantic Layer"]
        M1[Cube schemas\nmetric definitions]
        M2[Pre-aggregations\nmaterialized in storage]
        M3[REST · GraphQL · SQL API]
    end

    subgraph CONSUME["Consumption"]
        C1[Apache Superset\ndashboards]
        C2[Any BI tool\nCube SQL API]
        C3[Internal Apps\nCube REST API]
        C4[Notebooks\ndirect Trino SQL]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> T1 --> Z2 --> Z3
    T2 -. tests .-> Z3
    T3 -. docs .-> Z3
    Z3 -. queries .-> Q1
    Z3 -. defines .-> M1
    M1 --> M2 --> M3
    M3 --> C1 & C2 & C3
    Q1 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Sources (Any Cloud)"]
        A1[(RDBMS)]
        A2[SaaS]
        A3[Object Storage]
        A4[Kafka]
    end

    subgraph Ingestion
        B1[Airbyte ELT]
        B2[Debezium CDC]
        B3[Kafka Sink]
    end

    subgraph Storage["Object Storage + Iceberg"]
        C1[raw/]
        C2[intermediate/]
        C3[marts/]
    end

    subgraph Airflow["Airflow DAGs"]
        D1[ingest_trigger]
        D2[dbt_staging]
        D3[dbt_marts]
    end

    subgraph Cube["Cube.js"]
        E1[schemas]
        E2[pre-aggs]
        E3[API]
    end

    subgraph Consume
        F1[Superset]
        F2[BI via SQL]
        F3[REST apps]
    end

    A1 --> B2 --> C1
    A2 --> B1 --> C1
    A3 --> C1
    A4 --> B3 --> C1

    C1 --> D2 --> C2
    D2 -->|done| D3 --> C3

    C3 --> E1 --> E2 --> E3
    E3 --> F1 & F2 & F3
```

---

## Zone Design

```
Object Storage (S3 / ADLS / GCS) — Iceberg Tables
│
├── raw/
│   └── {source}/{table}/           — Parquet, Iceberg format, partitioned by date
│
├── intermediate/
│   └── {domain}/{entity}/          — dbt intermediate models, Iceberg
│
└── marts/
    ├── finance/
    │   └── fct_revenue/, dim_account/, dim_date/
    ├── growth/
    │   └── fct_signups/, dim_campaign/
    └── product/
        └── fct_events/, dim_user/, dim_feature/
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│  Trino + Cube.js Role Mapping + Object Storage IAM    │
│                                                       │
│  Role               Access Level    Scope             │
│  ────────────────   ────────────    ─────────────     │
│  data-engineer      Read + Write    All zones         │
│  analytics-eng      Read + Write    intermediate +    │
│                                     marts             │
│  bi-developer       Read only       marts only        │
│  business-user      Cube API only   defined cubes     │
│  app-service        REST API only   approved endpoints│
│                                                       │
│  Column masking   → Trino column masking rules        │
│  Row filtering    → Cube.js queryRewrite per role     │
│  Object ACL       → S3 bucket policies / ADLS RBAC   │
│  Secrets          → HashiCorp Vault / cloud KMS       │
│  Network          → mTLS between services on K8s      │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow Schedule\ndaily 02:00 UTC]

    T1 --> J1[Airbyte operator\ntrigger all syncs]
    J1 --> J2[Airflow sensor\nwait for Airbyte done]

    J2 --> J3[dbt BashOperator\ndbt run raw staging]
    J3 --> J4[dbt test\nsource freshness]
    J4 -->|pass| J5[dbt run\nintermediate models]
    J4 -->|fail| A1[Slack webhook\non_failure callback]

    J5 --> J6[dbt test\nintermediate assertions]
    J6 -->|pass| J7[dbt run\nmart models]
    J7 --> J8[dbt docs generate\nupload to object storage]
    J8 --> J9[Cube.js pre-agg warm\ncurl API endpoint]

    J6 -->|fail| A1
    J7 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | S3 / ADLS Gen2 / GCS | Portable; same Parquet + Iceberg everywhere |
| Table Format | Apache Iceberg | ACID, time-travel, schema evolution |
| Query Engine | Trino | Federated SQL over Iceberg; multi-catalog |
| Local / Dev Query | DuckDB | Sub-second on local Parquet; great for CI |
| Ingestion | Airbyte (Helm / Docker) | Self-hosted; 300+ connectors |
| CDC | Debezium → Kafka | Kafka topic per table; Iceberg sink connector |
| Transformation | dbt Core | Trino adapter; Iceberg materialization |
| Orchestration | Apache Airflow | KubernetesExecutor on K8s |
| Data Testing | dbt tests + Great Expectations | Schema + statistical assertions |
| Lineage | dbt docs + OpenLineage / Marquez | Emits lineage events from dbt + Airflow |
| Semantic Layer | Cube.js (Docker / K8s) | Pre-aggs in Iceberg; REST + SQL API |
| BI | Apache Superset | Connected to Cube SQL endpoint |
| Ad-hoc | Trino CLI / Jupyter | Direct SQL on Iceberg tables |
| Secrets | HashiCorp Vault | Dynamic secrets; cloud KMS integration |
| Catalog | Apache Iceberg REST Catalog / Nessie | Git-like branching with Nessie |
| Monitoring | OpenTelemetry → Grafana + Prometheus | All services instrumented |

---

## Comparison vs 10.7 (Multi-Cloud Managed)

| Dimension | 10.8 Multi-Cloud OSS | 10.7 Multi-Cloud Managed |
|-----------|---------------------|------------------------|
| Warehouse | Trino + Iceberg (OSS) | Snowflake (SaaS) |
| dbt runtime | dbt Core + Airflow | dbt Cloud |
| Semantic layer | Cube.js | dbt SL + AtScale |
| BI | Superset | Tableau Cloud |
| Cloud portability | 100% portable | Snowflake-dependent |
| Infra ops | High | Near-zero |
| Cost at scale | Lowest (no SaaS) | SaaS subscriptions |
| Vendor lock-in | None | Snowflake + dbt + Fivetran |

---

## When to Choose This Implementation

✅ Strict cloud-portability or multi-cloud mandate
✅ No SaaS vendor lock-in requirement
✅ Engineering team comfortable with K8s + distributed systems
✅ Very large scale where per-query SaaS costs are prohibitive
✅ Open source data stack as an organizational principle

❌ Small team with limited ops capacity → use 10.7 (managed)
❌ Microsoft 365 deeply embedded → 10.3 or 10.4
❌ Time-to-first-dashboard is critical → managed stacks are faster
