---
layout: default
title: "10.3 — Self-Serve Analytics Engineering · Azure Fully Managed"
---

# 10.3 — Self-Serve Analytics Engineering · Azure Fully Managed

**Stack:** Azure Synapse Analytics · dbt Cloud · Power BI (Semantic Model) · AtScale
**Processing:** Batch-first · On-Demand semantic queries
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Azure SQL / SQL Server]
        S2[SaaS Apps\nDynamics · Salesforce]
        S3[ADLS Gen2\nEvents · Files]
        S4[Event Hubs\nStreaming]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Azure Data Factory\nCopy + CDC pipelines]
        I2[Synapse Link\ndirect HTAP reads]
        I3[Fivetran / ADF\nSaaS connectors]
    end

    subgraph DWH["Synapse Dedicated SQL Pool"]
        R1[staging schema\nraw ingested data]
        R2[core schema\ndbt intermediate]
        R3[marts schema\nGold — facts + dims]
    end

    subgraph TRANSFORM["Transformation — dbt Cloud"]
        T1[dbt models\nSQL transforms]
        T2[dbt tests\nassertions]
        T3[dbt docs\nauto lineage]
    end

    subgraph SEMANTIC["Semantic Layer"]
        M1[Power BI Semantic Model\nDAX measures + dims]
        M2[AtScale\ncross-tool metric API]
    end

    subgraph CONSUME["Consumption"]
        C1[Power BI Service\ndashboards + reports]
        C2[Excel\nAnalyze in Excel]
        C3[Tableau / Looker\nAtScale JDBC]
        C4[Python / Notebooks\ndirect Synapse SQL]
    end

    SRC --> INGEST
    INGEST --> R1
    R1 --> T1 --> R2 --> R3
    T2 -. tests .-> R3
    T3 -. docs .-> R3
    R3 -. imports .-> M1
    R3 -. defines .-> M2
    M1 --> C1 & C2
    M2 --> C3
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
        B1[Azure Data Factory\nCopy Activity + CDC]
        B2[Fivetran]
    end

    subgraph Synapse["Synapse Dedicated SQL Pool"]
        C1[staging]
        C2[core]
        C3[marts]
    end

    subgraph dbt["dbt Cloud"]
        D1[staging models]
        D2[core models]
        D3[mart models]
    end

    subgraph Semantic["Semantic Layer"]
        E1[Power BI Dataset\nDAX model]
        E2[AtScale\nmetric API]
    end

    subgraph Consume
        F1[Power BI Service]
        F2[Excel]
        F3[Tableau / Looker\nAtScale]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B1

    C1 --> D1 --> C1
    D1 -->|success| D2 --> C2
    D2 -->|success| D3 --> C3

    C3 -->|DirectQuery| E1
    C3 --> E2

    E1 --> F1 & F2
    E2 --> F3
```

---

## Zone Design

```
Synapse Dedicated SQL Pool — Schemas
│
├── staging/
│   └── stg_{source}_{table}      — raw copy, column renames only
│
├── core/
│   └── int_{domain}_{entity}     — business logic, joins, dedupe
│
└── marts/
    ├── finance/
    │   └── fct_revenue, dim_cost_center, dim_date
    ├── sales/
    │   └── fct_opportunities, dim_account, dim_rep
    └── operations/
        └── fct_tickets, dim_product, dim_region
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│  Synapse RBAC + Power BI Row-Level Security               │
│                                                           │
│  Role                Access Level    Scope                │
│  ─────────────────   ────────────    ──────────────────   │
│  data-engineer       Read + Write    All schemas          │
│  analytics-eng       Read + Write    core + marts         │
│  bi-developer        Read only       marts only           │
│  business-analyst    Power BI RLS    filtered dataset     │
│  exec-viewer         Power BI RLS    summary metrics only │
│                                                           │
│  Column masking    → Synapse Dynamic Data Masking         │
│  Row security      → Power BI RLS DAX filter rules        │
│  AtScale rows      → queryRewrite per role                │
│  Secrets           → Azure Key Vault                      │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ dbt Cloud Schedule\ndaily 03:00 UTC]
    T2[📡 ADF Pipeline Complete\nevent trigger]

    T1 --> P1[ADF Pipeline\ncopy + CDC sync]
    P1 --> T2

    T2 --> J1[dbt run\nstaging models]
    J1 --> J2[dbt test\nsource freshness]
    J2 -->|pass| J3[dbt run\ncore models]
    J2 -->|fail| A1[Teams Alert\ndbt Cloud webhook]

    J3 --> J4[dbt test\ncore assertions]
    J4 -->|pass| J5[dbt run\nmart models]
    J5 --> J6[dbt docs generate]
    J6 --> J7[Power BI Dataset Refresh\nPower BI REST API trigger]

    J4 -->|fail| A1
    J5 -->|fail| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | Azure Synapse Dedicated SQL Pool | DWU-based scaling; pause/resume for cost |
| Ingestion — DB | Azure Data Factory | Copy Activity + CDC via Change Tracking |
| Ingestion — SaaS | Fivetran / ADF connectors | Managed connectors to 200+ SaaS sources |
| Synapse Link | Azure Synapse Link | Zero-ETL read from Cosmos DB / Dataverse |
| Transformation | dbt Cloud | Managed runtime; CI/CD + IDE |
| Data Testing | dbt tests | Schema + business-rule assertions |
| Lineage | dbt Docs + dbt Cloud Explorer | Column-level lineage |
| Semantic Layer — MS | Power BI Dataset (DAX) | Native Microsoft ecosystem |
| Semantic Layer — Multi | AtScale | For non-Power BI consumers |
| BI | Power BI Service | Embedded, mobile, paginated reports |
| Cross-tool BI | Tableau / Looker via AtScale | JDBC/SQL API connection |
| Ad-hoc | Synapse Studio SQL editor | Direct SQL for engineers |
| Secrets | Azure Key Vault | Referenced by ADF + dbt |
| Monitoring | Azure Monitor + dbt Cloud alerts | Pipeline failures → Teams |

---

## Comparison vs 10.4 (Azure OSS)

| Dimension | 10.3 Azure Managed | 10.4 Azure OSS |
|-----------|-------------------|---------------|
| dbt runtime | dbt Cloud (hosted) | dbt Core + Airflow on AKS |
| Semantic layer | Power BI Dataset + AtScale | Cube.js on AKS |
| BI | Power BI Service | Apache Superset |
| Microsoft integration | Native (Entra ID, Teams) | Partial |
| Infra ops | Near-zero | Moderate |
| Per-seat cost | Power BI Pro licensing | Lower OSS cost |
| Governance | Purview integration native | Manual tagging |

---

## When to Choose This Implementation

✅ Azure is primary cloud
✅ Microsoft 365 / Teams / Excel deeply embedded in org
✅ Power BI already licensed enterprise-wide
✅ Central data team < 5 engineers; low ops budget
✅ Dynamics 365 / Dataverse as key source (Synapse Link)

❌ Non-Microsoft BI tools mandated → use AtScale or Cube.js
❌ Cross-cloud portability needed → use 10.7 (Snowflake)
❌ Real-time operational analytics → Pattern 7
