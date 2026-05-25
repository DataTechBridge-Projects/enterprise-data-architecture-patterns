---
layout: default
title: "10.9 — Self-Serve Analytics Engineering · Hybrid OSS Self-Hosted"
---

# 10.9 — Self-Serve Analytics Engineering · Hybrid OSS Self-Hosted

**Stack:** PostgreSQL · dbt Core · Cube.js · Apache Superset · Apache Airflow on Kubernetes
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Full Build (self-hosted on-prem + cloud; zero SaaS dependency)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources (On-Prem + Cloud)"]
        S1[PostgreSQL / Oracle\non-prem OLTP]
        S2[SaaS APIs\ncloud sources]
        S3[File Shares\nCSV · Excel · XML]
        S4[Legacy Systems\nERP · mainframe extracts]
    end

    subgraph INGEST["Ingestion Layer — On-Prem K8s"]
        I1[Airbyte on K8s\nELT connectors]
        I2[Debezium on K8s\nCDC → Kafka]
        I3[Custom Python jobs\nlegacy extractors]
    end

    subgraph DWH["PostgreSQL Analytical DB"]
        R1[staging schema\nraw ELT data]
        R2[intermediate schema\ndbt models]
        R3[marts schema\nGold — facts + dims]
    end

    subgraph TRANSFORM["dbt Core + Airflow on K8s"]
        T1[dbt run\nSQL transforms]
        T2[dbt test\nassertions]
        T3[dbt docs\nlineage site]
    end

    subgraph SEMANTIC["Cube.js on K8s"]
        M1[Cube schemas\nmetric definitions]
        M2[Pre-aggregations\nPostgreSQL materialized views]
        M3[REST · GraphQL · SQL API]
    end

    subgraph CONSUME["Consumption"]
        C1[Apache Superset on K8s\ndashboards]
        C2[Any BI tool\nCube SQL API]
        C3[Internal Apps\nCube REST API]
        C4[Notebooks\ndirect Postgres SQL]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1 --> R2 --> R3
    T2 -. tests .-> R3
    T3 -. docs .-> R3
    R3 -. defines .-> M1
    M1 --> M2 --> M3
    M3 --> C1 & C2 & C3
    R3 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["On-Prem + Cloud Sources"]
        A1[(PostgreSQL / Oracle)]
        A2[SaaS APIs]
        A3[Files / ERP]
    end

    subgraph Ingestion["On-Prem K8s"]
        B1[Airbyte ELT]
        B2[Debezium CDC]
        B3[Custom scripts]
    end

    subgraph Postgres["PostgreSQL Analytical Schemas"]
        C1[staging]
        C2[intermediate]
        C3[marts]
    end

    subgraph Airflow["Airflow on K8s"]
        D1[ingest DAGs]
        D2[dbt staging]
        D3[dbt marts]
    end

    subgraph Cube["Cube.js on K8s"]
        E1[schemas]
        E2[pre-agg mat. views]
        E3[API]
    end

    subgraph Consume
        F1[Superset]
        F2[BI via SQL]
        F3[Apps REST]
    end

    A1 --> B2 --> C1
    A2 --> B1 --> C1
    A3 --> B3 --> C1

    C1 --> D2 --> C2
    D2 -->|done| D3 --> C3

    C3 --> E1 --> E2 --> E3
    E3 --> F1 & F2 & F3
```

---

## Zone Design

```
PostgreSQL Analytical Instance — Schemas
│
├── staging/
│   └── stg_{source}_{table}       — raw ELT copy, type casts only
│
├── intermediate/
│   └── int_{domain}_{entity}      — joins, business logic, dedupe
│
└── marts/
    ├── finance/
    │   └── fct_gl_lines, dim_cost_center, dim_date
    ├── hr/
    │   └── fct_headcount, dim_employee, dim_department
    └── operations/
        └── fct_work_orders, dim_asset, dim_location
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│  PostgreSQL Row/Col Security + Cube.js Role Mapping   │
│                                                       │
│  Role               Access Level    Scope             │
│  ────────────────   ────────────    ─────────────     │
│  data_engineer      Read + Write    All schemas       │
│  analytics_eng      Read + Write    intermediate +    │
│                                     marts             │
│  bi_developer       Read only       marts only        │
│  business_user      Cube API only   defined cubes     │
│  app_service        REST API only   approved endpoints│
│                                                       │
│  Column masking   → PostgreSQL column privileges      │
│  Row security     → PostgreSQL RLS policies           │
│  Cube rows        → queryRewrite per role             │
│  mTLS             → all K8s service-to-service        │
│  Secrets          → HashiCorp Vault on K8s            │
│  Audit logging    → pgaudit extension → SIEM          │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow Schedule\ndaily 01:00 local TZ]

    T1 --> J1[Airbyte operator\ntrigger all source syncs]
    J1 --> J2[Airflow sensor\nwait for Airbyte completion]

    J2 --> J3[dbt BashOperator\ndbt run staging]
    J3 --> J4[dbt test\nsource freshness]
    J4 -->|pass| J5[dbt run\nintermediate models]
    J4 -->|fail| A1[ops-channel\nSlack webhook / email]

    J5 --> J6[dbt test\nintermediate assertions]
    J6 -->|pass| J7[dbt run\nmart models]
    J7 --> J8[dbt test\nmart quality checks]
    J8 --> J9[REFRESH MATERIALIZED VIEW\nCube pre-agg views via psql]
    J9 --> J10[dbt docs generate\nserved via Nginx on K8s]

    J6 -->|fail| A1
    J7 -->|fail| A1
    J8 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Analytical Database | PostgreSQL 16 | Columnar extension (pg_columnstore / Citus) for larger datasets |
| CDC | Debezium on K8s | Postgres WAL → Kafka → staging schema |
| Ingestion | Airbyte (Helm on K8s) | Self-hosted; 300+ connectors |
| Legacy Extractors | Custom Python on K8s | CronJob pods for ERP / file-based sources |
| Transformation | dbt Core | PostgreSQL adapter; git-versioned |
| Orchestration | Apache Airflow (KubernetesExecutor) | Helm chart; one pod per task |
| Data Testing | dbt tests + dbt-expectations | Schema + custom SQL assertions |
| Pre-aggregations | PostgreSQL Materialized Views | Refreshed by Airflow after dbt run |
| Lineage | dbt docs + OpenLineage/Marquez | dbt + Airflow emit lineage events |
| Semantic Layer | Cube.js (Helm on K8s) | Connects to PostgreSQL; REST + SQL API |
| BI | Apache Superset (Helm on K8s) | SQLAlchemy → Cube SQL endpoint |
| Ad-hoc | psql / pgAdmin / Jupyter | Direct PostgreSQL access for engineers |
| Secrets | HashiCorp Vault on K8s | Dynamic PG credentials; Vault Agent sidecar |
| Monitoring | Prometheus + Grafana + Alertmanager | All services via Prometheus exporters |
| TLS/Auth | cert-manager + mTLS | Certificates for all service-to-service calls |

---

## Comparison vs 10.7 (Multi-Cloud Managed)

| Dimension | 10.9 Hybrid Self-Hosted | 10.7 Multi-Cloud Managed |
|-----------|------------------------|------------------------|
| Warehouse | PostgreSQL (on-prem K8s) | Snowflake (SaaS) |
| dbt runtime | dbt Core + Airflow | dbt Cloud |
| Semantic layer | Cube.js on K8s | dbt SL + AtScale |
| BI | Superset | Tableau Cloud |
| Infra ops | Highest | Near-zero |
| Cost at scale | Lowest (own hardware) | SaaS subscriptions |
| Data residency | 100% on-prem | Cloud-dependent |
| Internet connectivity | Not required | Required |
| Scale ceiling | PostgreSQL limits | Snowflake elastic |

---

## When to Choose This Implementation

✅ Air-gapped or strict data residency requirements
✅ On-prem Kubernetes already operational
✅ No internet connectivity to cloud SaaS providers
✅ Regulated industry where cloud egress is prohibited
✅ Existing PostgreSQL investment; want to maximize it

❌ Data volumes exceed PostgreSQL limits (>10TB analytical) → migrate to Trino + Iceberg (10.8)
❌ Cloud-first mandate → use 10.1, 10.3, or 10.5
❌ Small team with no K8s ops capacity → use a managed cloud offering
❌ Real-time analytics → Pattern 7
