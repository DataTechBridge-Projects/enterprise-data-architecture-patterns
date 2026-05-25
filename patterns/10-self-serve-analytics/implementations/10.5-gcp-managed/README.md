---
layout: default
title: "10.5 — Self-Serve Analytics Engineering · GCP Fully Managed"
---

# 10.5 — Self-Serve Analytics Engineering · GCP Fully Managed

**Stack:** BigQuery · dbt Cloud · Looker (LookML semantic layer) · Looker Studio
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Cloud SQL / AlloyDB]
        S2[SaaS Apps\nSalesforce · Marketo]
        S3[GCS\nFiles · Events]
        S4[Pub/Sub\nStreaming]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Datastream\nCDC from Cloud SQL]
        I2[Fivetran / BigQuery DTS\nSaaS connectors]
        I3[Dataflow\nPub/Sub → BigQuery]
    end

    subgraph DWH["BigQuery — Storage & Compute"]
        R1[raw dataset\ningested data]
        R2[intermediate dataset\ndbt models]
        R3[marts dataset\nGold — facts + dims]
    end

    subgraph TRANSFORM["Transformation — dbt Cloud"]
        T1[dbt models\nSQL transforms]
        T2[dbt tests\nassertions]
        T3[dbt docs\nauto lineage]
    end

    subgraph SEMANTIC["Semantic Layer — Looker"]
        M1[LookML Models\ndimensions + measures]
        M2[Explores\njoin definitions]
        M3[Looker API\nembedded + headless]
    end

    subgraph CONSUME["Consumption"]
        C1[Looker Dashboards\nbusiness self-serve]
        C2[Looker Studio\nlight reports]
        C3[Sheets / Excel\nLooker Connected Sheets]
        C4[APIs / Apps\nLooker Embedded]
        C5[Notebooks\ndirect BQ SQL]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1 --> R2 --> R3
    T2 -. tests .-> R3
    T3 -. docs .-> R3
    R3 -. LookML view .-> M1
    M1 --> M2 --> M3
    M3 --> C1
    M3 --> C2
    M3 --> C3
    M3 --> C4
    R3 --> C5
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Cloud SQL / AlloyDB)]
        A2[SaaS APIs]
        A3[GCS / Pub/Sub]
    end

    subgraph Ingestion
        B1[Datastream\nCDC]
        B2[Fivetran / BigQuery DTS]
        B3[Dataflow]
    end

    subgraph BigQuery["BigQuery Datasets"]
        C1[raw]
        C2[intermediate]
        C3[marts]
    end

    subgraph dbt["dbt Cloud"]
        D1[staging models]
        D2[intermediate models]
        D3[mart models]
    end

    subgraph Looker["Looker Semantic Layer"]
        E1[LookML Views\nfrom marts]
        E2[LookML Explores\njoin logic]
        E3[Looker API]
    end

    subgraph Consume
        F1[Looker Dashboards]
        F2[Looker Studio]
        F3[Connected Sheets]
        F4[Embedded Apps]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1

    C1 --> D1 --> C1
    D1 -->|success| D2 --> C2
    D2 -->|success| D3 --> C3

    C3 -->|view files| E1 --> E2 --> E3

    E3 --> F1 & F2 & F3 & F4
```

---

## Zone Design

```
BigQuery Datasets (per project or dataset isolation)
│
├── raw/
│   └── {source}_{table}           — raw ingested, no transforms
│
├── intermediate/
│   └── int_{domain}_{entity}      — joins, business logic, dedupe
│
└── marts/
    ├── finance/
    │   └── fct_revenue, dim_account, dim_date
    ├── marketing/
    │   └── fct_campaigns, dim_channel, dim_audience
    └── product/
        └── fct_events, dim_feature, dim_user
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│  BigQuery IAM + Looker Access Controls                    │
│                                                           │
│  Role                Access Level    Scope                │
│  ─────────────────   ────────────    ──────────────────   │
│  data-engineer       Read + Write    All datasets         │
│  analytics-eng       Read + Write    intermediate + marts │
│  bi-developer        Read only       marts only           │
│  business-analyst    Looker access   defined explores     │
│  exec-viewer         Looker access   curated dashboards   │
│                                                           │
│  Column masking    → BigQuery column-level security       │
│  Row filtering     → Looker access_filter in LookML       │
│  Data policies     → BigQuery data policies (tag-based)   │
│  Secrets           → Secret Manager                       │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ dbt Cloud Schedule\ndaily 03:00 UTC]
    T2[📡 Datastream / Fivetran Done\nCloud Pub/Sub event]

    T1 --> J1[dbt run\nstaging models]
    T2 --> J1

    J1 --> J2[dbt test\nsource freshness]
    J2 -->|pass| J3[dbt run\nintermediate models]
    J2 -->|fail| A1[Slack alert\ndbt Cloud webhook]

    J3 --> J4[dbt test\nintermediate]
    J4 -->|pass| J5[dbt run\nmart models]
    J5 --> J6[dbt docs generate]
    J6 --> J7[Looker PDT rebuild\nLooker API trigger]

    J4 -->|fail| A1
    J5 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | BigQuery | Serverless; slot reservations for predictable cost |
| CDC | Datastream | Cloud SQL / AlloyDB → BigQuery direct |
| SaaS Ingestion | Fivetran / BigQuery DTS | DTS for Google products; Fivetran for others |
| Stream Ingestion | Dataflow (Apache Beam) | Pub/Sub → BigQuery streaming insert |
| Transformation | dbt Cloud | BigQuery adapter; partitioned model support |
| Data Testing | dbt tests + re_data | Schema + anomaly detection |
| Lineage | dbt Docs + dbt Cloud Explorer | Column-level lineage |
| Semantic Layer | Looker (LookML) | Native BigQuery integration; PDTs; explores |
| BI — Self-Serve | Looker Dashboards | Business user self-service |
| BI — Light | Looker Studio | Free; embeds in Google Workspace |
| BI — Spreadsheets | Connected Sheets | Google Sheets ↔ BigQuery / Looker |
| Embedded | Looker Embedded API | Customer-facing analytics |
| Orchestration | dbt Cloud Scheduler | Job chaining; dependency graph |
| Secrets | GCP Secret Manager | Referenced by dbt + Datastream |
| Monitoring | Cloud Monitoring + dbt Cloud alerts | Query costs + job failures |

---

## Comparison vs 10.6 (GCP OSS)

| Dimension | 10.5 GCP Managed | 10.6 GCP OSS |
|-----------|-----------------|-------------|
| dbt runtime | dbt Cloud | dbt Core + Airflow on GKE |
| Semantic layer | Looker LookML | Cube.js on GKE |
| BI | Looker + Looker Studio | Apache Superset |
| Google Workspace integration | Native (Sheets, Slides) | None |
| Infra ops | Near-zero | Moderate (GKE) |
| Cost at scale | Looker licensing | Compute-based |
| Embedded analytics | Looker Embedded API | Superset embedded |

---

## When to Choose This Implementation

✅ GCP is primary cloud
✅ Google Workspace (Sheets, Slides, Docs) deeply embedded in org
✅ BigQuery already the central warehouse
✅ Looker licensing is acceptable
✅ Want zero infra ops for transformation + BI

❌ Non-Google BI tools mandated → add AtScale or Cube.js layer
❌ Cross-cloud portability required → use 10.7 (Snowflake)
❌ Real-time operational analytics → Pattern 7
