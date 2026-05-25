---
layout: default
title: "10.1 — Self-Serve Analytics Engineering · AWS Fully Managed"
---

# 10.1 — Self-Serve Analytics Engineering · AWS Fully Managed

**Stack:** Redshift · dbt Cloud · AtScale · Tableau / Looker · AWS Glue Catalog
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[RDS / Aurora]
        S2[SaaS Apps\nSalesforce · HubSpot]
        S3[S3 Data Lake\nEvents · Logs]
        S4[Third-Party APIs]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[AWS DMS\nCDC from OLTP]
        I2[AWS Glue ETL\nSaaS / File loads]
        I3[Fivetran / Airbyte\nManaged connectors]
    end

    subgraph DWH["Redshift — Storage & Compute"]
        R1[Staging Schema\nraw / untransformed]
        R2[Core Schema\ndbt intermediate models]
        R3[Marts Schema\ndbt Gold — facts + dims]
    end

    subgraph TRANSFORM["Transformation — dbt Cloud"]
        T1[dbt Models\nSQL transforms]
        T2[dbt Tests\ndata quality assertions]
        T3[dbt Docs\nauto-generated lineage]
    end

    subgraph SEMANTIC["Semantic / Metric Layer — AtScale"]
        M1[Metric Definitions\nrevenue · churn · LTV]
        M2[Dimension Catalog\ndate · customer · product]
        M3[Access Rules\nrow/col security]
    end

    subgraph CONSUME["Consumption"]
        C1[Tableau\ndashboards · workbooks]
        C2[Looker\nLookML + explores]
        C3[Excel / Google Sheets\nAtScale JDBC]
        C4[Python / Jupyter\nBI & ad-hoc]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1
    T1 --> T2
    T1 --> R2 --> R3
    T3 -. lineage .-> R3
    R3 -. registers .-> M1
    M2 & M3 -. enforces .-> M1
    M1 --> C1
    M1 --> C2
    M1 --> C3
    R3 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[SaaS APIs]
        A3[S3 Lake]
    end

    subgraph Ingestion
        B1[AWS DMS\nCDC]
        B2[Glue ETL / Fivetran]
    end

    subgraph Redshift["Redshift Schemas"]
        C1[staging\nraw copy]
        C2[core\nintermediate]
        C3[marts\nGold — facts/dims]
    end

    subgraph dbt["dbt Cloud"]
        D1[dbt run\nSQL models]
        D2[dbt test\nassertions]
        D3[dbt docs\nlineage catalog]
    end

    subgraph Semantic["AtScale Semantic Layer"]
        E1[Metric Definitions]
        E2[Dimension Catalog]
    end

    subgraph Consume
        F1[Tableau]
        F2[Looker]
        F3[Excel / Sheets]
        F4[Python ad-hoc]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B2

    C1 -->|dbt run| C2 -->|dbt run| C3
    C3 --> D2
    C3 --> D3

    C3 -->|registers metrics| E1
    E2 --> E1

    E1 --> F1
    E1 --> F2
    E1 --> F3
    C3 --> F4
```

---

## Zone Design

```
Redshift Schemas
│
├── staging/
│   └── stg_{source}_{table}    — exact copy, renamed columns only
│
├── core/
│   └── int_{domain}_{entity}   — joins, deduplication, business logic
│
└── marts/
    ├── finance/
    │   └── fct_revenue, dim_customer, dim_date
    ├── marketing/
    │   └── fct_campaigns, dim_channel
    └── product/
        └── fct_events, dim_feature
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│  AtScale + Redshift RBAC                              │
│                                                       │
│  Role               Access Level    Scope             │
│  ────────────────   ────────────    ─────────────     │
│  data-engineer      Read + Write    All schemas       │
│  analytics-eng      Read + Write    core + marts      │
│  bi-developer       Read only       marts only        │
│  business-analyst   Metric API      AtScale only      │
│  exec-viewer        Metric API      filtered metrics  │
│                                                       │
│  Column masking  → PII via Redshift Dynamic Masking  │
│  Row security    → AtScale MDX filter per role        │
│  dbt exposures   → declare who consumes each model    │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ dbt Cloud Schedule\ndaily 03:00 UTC]
    T2[🔀 Fivetran Sync Complete\nevent trigger]

    T1 --> J1[dbt run\nstaging models]
    T2 --> J1

    J1 --> J2[dbt test\nsource freshness + assertions]
    J2 -->|pass| J3[dbt run\ncore models]
    J2 -->|fail| A1[Slack Alert\ndbt Cloud notification]

    J3 --> J4[dbt test\ncore assertions]
    J4 -->|pass| J5[dbt run\nmart models]
    J5 --> J6[dbt docs generate\nlineage refresh]
    J6 --> J7[AtScale cache warm\npre-aggregate metrics]

    J4 -->|fail| A1
    J5 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | Amazon Redshift | RA3 nodes; storage-compute separation |
| DB Ingestion | AWS DMS | CDC from RDS/Aurora |
| SaaS Ingestion | Fivetran / AWS Glue | Fivetran for SaaS; Glue for custom |
| Transformation | dbt Cloud | Managed dbt; CI/CD, schedules, IDE |
| Data Testing | dbt tests + Great Expectations | Schema + business-rule assertions |
| Lineage & Docs | dbt Docs (auto-gen) | Column-level lineage in dbt Cloud |
| Semantic Layer | AtScale (Cube Cloud alt.) | MDX/SQL; connects Tableau + Excel + Looker |
| BI — Dashboard | Tableau Cloud | Live AtScale connection; no extracts needed |
| BI — Explore | Looker | LookML optional; or pure AtScale JDBC |
| Ad-hoc | Python / Jupyter + Redshift | Direct SQL for power users |
| Orchestration | dbt Cloud Scheduler | Job chaining + freshness checks |
| Secrets / Auth | AWS Secrets Manager | Redshift credentials for dbt + Fivetran |
| Monitoring | CloudWatch + dbt Cloud alerts | Query performance + job failures |

---

## Comparison vs 10.2 (AWS OSS)

| Dimension | 10.1 AWS Managed | 10.2 AWS OSS |
|-----------|-----------------|--------------|
| dbt runtime | dbt Cloud (hosted) | dbt Core + Airflow (self-managed) |
| Semantic layer | AtScale SaaS | Cube.js on EC2/ECS |
| BI tool | Tableau Cloud | Apache Superset |
| Infra ops | Near-zero | Moderate (Airflow + Cube.js) |
| Cost model | Subscription per seat | Compute-based; lower per-seat |
| CI/CD for dbt | dbt Cloud native | GitHub Actions + Airflow |
| Lineage UI | dbt Cloud explorer | dbt docs static site |

---

## When to Choose This Implementation

✅ AWS is primary cloud
✅ Business teams need governed self-serve without SQL knowledge
✅ Consistent metrics across Tableau, Excel, and Looker matter
✅ Small central data team; low infra ops budget
✅ dbt Cloud licensing is acceptable

❌ Need sub-second latency → Pattern 7 (Operational Analytics)
❌ Heavy ML feature engineering → Pattern 8 (AI/ML Platform)
❌ Multi-cloud portability required → use 10.7 or 10.8
