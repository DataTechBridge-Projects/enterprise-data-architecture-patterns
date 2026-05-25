---
layout: default
title: "10.6 — Self-Serve Analytics Engineering · GCP OSS on Cloud"
---

# 10.6 — Self-Serve Analytics Engineering · GCP OSS on Cloud

**Stack:** BigQuery · dbt Core · Cube.js · Apache Superset · Apache Airflow
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Build (OSS on GCP infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Cloud SQL / Spanner]
        S2[SaaS APIs]
        S3[GCS\nFiles · Events]
        S4[Pub/Sub Streams]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Airbyte on GKE\nELT connectors]
        I2[Datastream\nCDC → BigQuery]
        I3[Dataflow\nPub/Sub → BQ]
    end

    subgraph DWH["BigQuery — Storage & Compute"]
        R1[raw dataset]
        R2[intermediate dataset]
        R3[marts dataset]
    end

    subgraph TRANSFORM["dbt Core + Airflow on GKE"]
        T1[dbt run\nSQL models]
        T2[dbt test\nassertions]
        T3[dbt docs\nlineage]
    end

    subgraph SEMANTIC["Cube.js on GKE"]
        M1[Cube schemas\nmetric definitions]
        M2[Pre-aggregations\nBQ materialized]
        M3[REST · GraphQL · SQL API]
    end

    subgraph CONSUME["Consumption"]
        C1[Apache Superset\ndashboards]
        C2[Any BI tool\nCube SQL API]
        C3[Internal Apps\nCube REST API]
        C4[Notebooks\ndirect BQ SQL]
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
    subgraph Sources
        A1[(Cloud SQL)]
        A2[SaaS]
        A3[GCS / Pub/Sub]
    end

    subgraph Ingestion
        B1[Airbyte on GKE]
        B2[Datastream CDC]
        B3[Dataflow]
    end

    subgraph BigQuery["BigQuery Datasets"]
        C1[raw]
        C2[intermediate]
        C3[marts]
    end

    subgraph Airflow["Airflow on GKE"]
        D1[ingest_sync]
        D2[dbt_staging]
        D3[dbt_marts]
    end

    subgraph Cube["Cube.js on GKE"]
        E1[schemas]
        E2[pre-aggs]
        E3[API]
    end

    subgraph Consume
        F1[Superset]
        F2[BI via SQL API]
        F3[REST apps]
    end

    A1 --> B2 --> C1
    A2 --> B1 --> C1
    A3 --> B3 --> C1

    C1 --> D2 --> C1
    D2 -->|done| D3 --> C3

    C3 --> E1 --> E2 --> E3
    E3 --> F1 & F2 & F3
```

---

## Zone Design

```
BigQuery Datasets
│
├── raw/
│   └── {source}_{table}           — raw ELT copy
│
├── intermediate/
│   └── int_{domain}_{entity}      — joins, business logic
│
└── marts/
    ├── finance/
    │   └── fct_revenue, dim_gl_account, dim_date
    ├── growth/
    │   └── fct_signups, dim_campaign, dim_channel
    └── product/
        └── fct_events, dim_user, dim_feature
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│  BigQuery IAM + Cube.js Role Mapping                  │
│                                                       │
│  Role               Access Level    Scope             │
│  ────────────────   ────────────    ─────────────     │
│  data-engineer      Read + Write    All datasets      │
│  analytics-eng      Read + Write    intermediate +    │
│                                     marts             │
│  bi-developer       Read only       marts only        │
│  business-user      Cube API only   defined cubes     │
│  app-service        REST API only   approved endpoints│
│                                                       │
│  Column security  → BQ column-level IAM               │
│  Row filtering    → Cube.js queryRewrite              │
│  Secrets          → GCP Secret Manager + Airflow KV   │
│  Network          → GKE private cluster + VPC         │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow Schedule\ndaily 02:00 UTC]

    T1 --> J1[Airbyte operator\ntrigger syncs]
    J1 --> J2[Airflow sensor\nwait for completion]

    J2 --> J3[dbt BashOperator\ndbt run raw/staging]
    J3 --> J4[dbt test\nsource freshness]
    J4 -->|pass| J5[dbt run\nintermediate]
    J4 -->|fail| A1[Slack webhook\nAirflow on_failure]

    J5 --> J6[dbt test]
    J6 -->|pass| J7[dbt run\nmart models]
    J7 --> J8[dbt docs generate\nGCS static site]
    J8 --> J9[Cube.js refresh\ncurl pre-agg endpoint]

    J6 -->|fail| A1
    J7 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | BigQuery | Serverless; on-demand or slots |
| CDC | Datastream | Cloud SQL → BigQuery direct |
| SaaS / File Ingestion | Airbyte on GKE | Helm chart; 300+ connectors |
| Stream Ingestion | Dataflow (Apache Beam) | Pub/Sub → BigQuery |
| Transformation | dbt Core | CLI; run via Airflow BashOperator |
| Orchestration | Apache Airflow on GKE | KubernetesExecutor; Helm chart |
| Data Testing | dbt tests + Elementary | Schema + anomaly detection |
| Lineage | dbt docs (GCS static site) | Cloud CDN fronted |
| Semantic Layer | Cube.js on GKE | Pre-aggregations in BQ; REST + SQL API |
| BI | Apache Superset on GKE | Connected to Cube SQL API |
| Ad-hoc | Vertex AI Workbench / Colab | Direct BQ SQL + BigQuery storage API |
| Secrets | GCP Secret Manager | Airflow KV backend |
| Monitoring | Cloud Monitoring + PagerDuty | DAG failures + BQ slot utilization |

---

## Comparison vs 10.5 (GCP Managed)

| Dimension | 10.6 GCP OSS | 10.5 GCP Managed |
|-----------|-------------|-----------------|
| dbt runtime | dbt Core + Airflow | dbt Cloud |
| Semantic layer | Cube.js on GKE | Looker LookML |
| BI | Superset | Looker + Looker Studio |
| Google Workspace integration | None native | Native (Sheets, Slides) |
| Infra ops | Moderate (GKE + Airflow) | Near-zero |
| Cost at scale | Lower (no Looker licensing) | Looker per-seat |
| Customization | High | LookML constraints |

---

## When to Choose This Implementation

✅ GCP primary cloud, want full OSS flexibility
✅ Engineering team already runs GKE workloads
✅ Looker licensing cost is prohibitive
✅ Need custom Cube.js API integrations for product analytics
✅ Airbyte used across other patterns in the estate

❌ Google Workspace self-serve is the primary use case → 10.5
❌ No GKE ops capacity → use 10.5 (managed)
❌ Real-time analytics → Pattern 7
