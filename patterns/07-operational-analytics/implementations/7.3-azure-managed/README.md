---
layout: default
title: "7.3 — Operational Analytics · Azure Fully Managed"
---

# 7.3 — Operational Analytics · Azure Fully Managed

**Stack:** Azure SQL · Azure Cosmos DB · Synapse Link · Azure Synapse Analytics · Power BI
**Processing:** Streaming / Near-Real-Time · HTAP + ODS pattern
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources"]
        S1[Azure SQL Database\nOrders · CRM · Finance]
        S2[Cosmos DB\nProduct · Session · Events]
        S3[Azure SQL Managed Instance\nLegacy ERP]
        S4[Event Hubs\nApp telemetry]
    end

    subgraph CDC["CDC / Sync Layer"]
        I1[Azure SQL CDC\n→ Synapse Link for SQL]
        I2[Cosmos DB Analytical Store\nSynapse Link — zero ETL]
        I3[ADF Copy Activity\nScheduled or trigger-based]
        I4[Event Hubs Capture\n→ ADLS Gen2]
    end

    subgraph ODS["ODS — Azure Synapse Analytics"]
        O1[Staging Pool\nraw CDC rows]
        O2[Dedicated SQL Pool\nODS current state]
        O3[Serverless Pool\naggregates on ADLS]
    end

    subgraph CATALOG["Catalog & Governance\nMicrosoft Purview"]
        C1[Purview Catalog\ntable lineage + tags]
        C2[Synapse RBAC\nworkspace access]
        C3[Azure AD Groups\ncol/row security]
    end

    subgraph CONSUME["Consumption"]
        F1[Power BI\nOperational Dashboards]
        F2[Synapse Serverless SQL\nAd-hoc queries]
        F3[Azure Analysis Services\nSemantic layer]
        F4[Azure Monitor Alerts\nOps notifications]
    end

    SRC --> CDC
    I1 --> O1
    I2 --> O2
    I3 --> O1
    I4 --> O3
    O1 --> O2
    O2 & O3 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1
    C2 --> F2
    O2 --> F3
    O3 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Azure SQL DB)]
        A2[(Cosmos DB)]
        A3[(SQL Managed Instance)]
        A4[Event Hubs]
    end

    subgraph Sync
        B1[Synapse Link for SQL\nauto CDC → Synapse]
        B2[Synapse Link for Cosmos\nanalytical store sync]
        B3[ADF Copy Activity\nbatch / trigger]
        B4[Event Hubs Capture\nAvro → ADLS]
    end

    subgraph Synapse["Azure Synapse Analytics"]
        C1[Dedicated SQL Pool\nODS current state]
        C2[Serverless SQL Pool\nagg views on ADLS]
    end

    subgraph Catalog["Microsoft Purview"]
        D1[Data Catalog\nlineage + classification]
        D2[Synapse RBAC\naccess enforcement]
    end

    subgraph Consume
        E1[Power BI\nDashboards]
        E2[Synapse Serverless\nAd-hoc SQL]
        E3[Analysis Services\nSemantic model]
        E4[Azure Monitor\nAlerts]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> B4 --> C2

    C1 & C2 -->|lineage| D1 --> D2

    D2 -->|reads| E1
    D2 -->|reads| E2
    C1 -->|import| E3
    C2 -->|threshold| E4
```

---

## Zone Design

```
ADLS Gen2: adls://<company>-ops/
│
├── landing/
│   └── {source}/{entity}/year=YYYY/month=MM/day=DD/
│       Event Hubs Capture (Avro) + ADF raw copies
│
├── ods/
│   └── {domain}/{entity}/                -- Parquet; Synapse external tables
│       Updated by Synapse Link or ADF trigger
│
└── agg/
    └── {domain}/{metric}/{grain}/        -- Parquet; Synapse Serverless views
        Power BI DirectQuery target

Synapse Dedicated SQL Pool (ods_db):
  └── schema: ods   → current state tables (CTAS + MERGE)
  └── schema: stage → raw CDC rows from Synapse Link
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│         Microsoft Purview + Azure AD + Synapse RBAC   │
│                                                       │
│  Azure AD Group / SP      Access Level   Layer        │
│  ──────────────────────   ────────────   ──────────   │
│  sg-ops-engineer          Read + Write   All pools    │
│  sg-ops-analyst           Read only      ods · agg    │
│  sg-bi-consumer           Read only      agg (PBI)    │
│  sg-app-service           Read only      ods (cols)   │
│  synapse-link-svc-prin    Write only     stage schema  │
│                                                       │
│  Column masking  → Synapse Dynamic Data Masking        │
│  Row security    → Synapse Row-Level Security (RLS)    │
│  Encryption      → Azure KMS (CMK) on ADLS + Synapse  │
│  Network         → Private Endpoint + Managed VNet     │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Synapse Link\ncontinuous auto-sync]
    T2[⏰ ADF Trigger\nevery 15 min]
    T3[⏰ Synapse Pipeline\nMERGE every 5 min]
    T4[⏰ Power BI Refresh\nevery 15 min]

    T1 -->|change feed| J1[Cosmos Analytical Store\n→ Synapse ODS auto-sync]
    T2 --> J2[ADF Copy\nSQL MI → stage schema]
    J2 --> J3[Synapse Stored Proc\nMERGE stage → ods]
    T3 --> J3
    J3 --> J4[Synapse Serverless View\nagg refresh — CTAS]
    J4 --> T4

    J3 -->|fail| A1[Azure Monitor Alert\n→ Teams / PagerDuty]
    T1 -->|lag > 2 min| A1
```

---

## Component Map

| Component | Azure Service | Notes |
|-----------|--------------|-------|
| OLTP Source (relational) | Azure SQL Database | CDC enabled; Synapse Link connector |
| OLTP Source (NoSQL) | Cosmos DB | Analytical store enabled; zero ETL to Synapse |
| Legacy OLTP | SQL Managed Instance | ADF CDC connector or bulk copy |
| App Events | Event Hubs + Capture | Avro to ADLS Gen2 |
| CDC / Sync | Synapse Link for SQL / Cosmos | Auto-sync; no ETL pipeline needed |
| Batch Ingestion | Azure Data Factory | Fallback for unsupported sources |
| ODS Store | Synapse Dedicated SQL Pool | MERGE-based current state |
| Aggregate Layer | Synapse Serverless SQL Pool | Views on Parquet in ADLS |
| Catalog | Microsoft Purview | Auto-scan + lineage + classification |
| Access Control | Synapse RBAC + Azure AD | DDM + RLS in Synapse |
| BI / Dashboards | Power BI Premium | DirectQuery + Import mode |
| Semantic Layer | Azure Analysis Services | Tabular model on top of Synapse |
| Ad-hoc SQL | Synapse Serverless SQL | Pay-per-query on ADLS Parquet |
| Alerting | Azure Monitor + Action Groups | Metric/log alerts → Teams/PagerDuty |
| Encryption | Azure Key Vault + CMK | ADLS + Synapse encrypted at rest |

---

## Comparison vs 7.4 (Azure OSS)

| Dimension | 7.3 Azure Fully Managed | 7.4 Azure OSS |
|-----------|------------------------|---------------|
| CDC Engine | Synapse Link (zero ETL) | Debezium on AKS |
| Message Bus | None (Synapse Link) | Event Hubs (Kafka API) |
| Stream Processor | Synapse pipelines | Apache Flink on AKS |
| ODS Store | Synapse Dedicated Pool | Delta Lake on ADLS |
| Latency | ~1–5 min | ~5–15 s |
| Ops Overhead | Low | High |
| Cost Model | DTU/vCore + DWU | AKS + Event Hubs + compute |
| Open Format | No (Synapse proprietary) | Yes (Delta Lake) |

---

## When to Choose This Implementation

✅ Azure is primary cloud with Azure SQL + Cosmos DB sources  
✅ Power BI is the primary BI tool  
✅ Want zero CDC pipeline maintenance (Synapse Link)  
✅ Purview already deployed for governance  
✅ Sub-5-minute latency is acceptable  

❌ Need sub-30-second latency → use 7.4 (Flink on AKS)  
❌ Open format required → use 7.4 (Delta Lake)  
❌ Sources outside Azure SQL / Cosmos DB ecosystem → use 7.4 (Debezium)
