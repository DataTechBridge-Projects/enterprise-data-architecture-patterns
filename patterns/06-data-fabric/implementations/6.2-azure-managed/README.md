---
layout: default
title: "6.2 — Data Fabric · Azure Fully Managed"
---

# 6.2 — Data Fabric · Azure Fully Managed

**Stack:** Microsoft Purview · Azure Synapse Link · Synapse Serverless SQL · Azure API Management · Microsoft Defender for Cloud
**Processing:** Federated Query · No Data Movement · Active Metadata · Zero-ETL Operational Analytics
**Buy vs Build:** Buy (fully managed Azure-native fabric)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Physical Data Sources — Data Stays In Place"]
        S1[(Azure SQL DB / SQL Server\noperational)]
        S2[(Cosmos DB\nNoSQL · multi-model)]
        S3[ADLS Gen2\ndata lake · Delta / Parquet]
        S4[(Synapse Dedicated Pool\ndata warehouse)]
        S5[On-prem / Other\nvia Purview connector]
    end

    subgraph META["Active Metadata — Microsoft Purview"]
        M1[Purview Scanning\nauto-discover schema · lineage · classification]
        M2[Purview Data Map\ntechnical + business metadata · asset graph]
        M3[Purview Data Catalog\ndata product discovery · business glossary]
        M4[Purview DLP\nPII / PHI / PCI sensitivity labels · auto-tag]
    end

    subgraph LINK["Zero-ETL Operational Link — Synapse Link"]
        L1[Synapse Link for Cosmos DB\nanalytical store · no ETL]
        L2[Synapse Link for SQL DB\nchange feed → analytical endpoint]
    end

    subgraph GOV["Governance & Policy"]
        G1[Purview Data Policy\naccess policies enforced at source]
        G2[Azure RBAC + ABAC\nRole + attribute conditions]
        G3[Microsoft Defender for Cloud\nthreat detection · compliance posture]
    end

    subgraph QUERY["Federated Virtual Query — Synapse Serverless SQL"]
        Q1[Synapse Serverless SQL\nquery ADLS · Cosmos · SQL via OPENROWSET]
        Q2[Azure API Management\nexpose data products as REST APIs]
    end

    subgraph CONSUME["Consumers"]
        F1[Power BI\nDirectLake · Synapse connection]
        F2[Azure ML\nSilverberry via Synapse or ADLS]
        F3[App Teams\nREST API via APIM]
        F4[Purview Portal\nself-serve discovery]
    end

    SRC -. scan .-> M1
    M1 --> M2 --> M3
    M2 -. classify .-> M4
    S2 --> L1
    S1 --> L2
    M3 -. policies .-> G1
    G1 --> Q1
    L1 & L2 --> Q1
    Q1 --> Q2
    Q1 --> F1 & F2
    Q2 --> F3
    M3 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Physical Data — In-Place"]
        A1[(Azure SQL DB)]
        A2[(Cosmos DB)]
        A3[ADLS Gen2]
        A4[(Synapse DW)]
    end

    subgraph Metadata["Microsoft Purview"]
        B1[Purview Scanner\nauto schema + lineage]
        B2[Data Map\nasset graph]
        B3[Data Catalog\nbusiness glossary]
        B4[DLP\nsensitivity labels]
    end

    subgraph Link["Synapse Link"]
        C1[Cosmos DB Analytical Store\nzero-ETL]
        C2[SQL DB analytical endpoint\nchange feed]
    end

    subgraph Policy["Access Policy"]
        D1[Purview Data Policy\nsource-enforced]
        D2[Azure RBAC\nrole conditions]
    end

    subgraph Query["Synapse Serverless SQL"]
        E1[OPENROWSET\nfederated query]
        E2[APIM\nREST data products]
    end

    subgraph Consume
        F1[Power BI\ndashboards]
        F2[Azure ML\ntraining]
        F3[Apps\nREST API]
    end

    A1 & A2 & A3 & A4 -. scan .-> B1
    B1 --> B2 --> B3
    A1 & A3 -. scan .-> B4
    A2 --> C1
    A1 --> C2
    B3 -. policies .-> D1 --> D2
    D2 --> E1
    C1 & C2 --> E1
    E1 --> E2
    E1 --> F1 & F2
    E2 --> F3
```

---

## Catalog Structure

```
Microsoft Purview Data Map
├── Collection: azure-data-fabric
│   ├── Source: Azure SQL DB (prod-sql-server)
│   │   ├── Asset: dbo.customers        → sensitivity: PII
│   │   └── Asset: dbo.orders           → sensitivity: Confidential
│   ├── Source: Cosmos DB (cosmos-ops)
│   │   ├── Asset: orders container     → analytical store enabled
│   │   └── Asset: inventory container  → analytical store enabled
│   ├── Source: ADLS Gen2 (datalake-prod)
│   │   ├── Asset: /finance/*/          → Delta Lake
│   │   └── Asset: /marketing/*/        → Parquet
│   └── Source: Synapse DW (synapse-prod)
│       └── Asset: gold.* tables        → BI-ready

Business Glossary (Purview)
  customer → canonical definition · owner: CRM team
  order    → canonical definition · owner: Commerce team

All assets are virtual — Purview maps metadata only.
Synapse Link provides zero-ETL analytical access to Cosmos DB and SQL DB.
Synapse Serverless SQL queries ADLS in-place via OPENROWSET.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Purview Data Policy + Azure RBAC + Defender for Cloud           │
│                                                                  │
│  Principal               Access Level     Scope                  │
│  ──────────────────────  ─────────────    ──────────────────── │
│  data-engineer-grp       READ + scan      all collections        │
│  bi-analyst-grp          READ (masked)    finance · marketing    │
│  data-scientist-grp      READ             ADLS full · Synapse    │
│  app-team-sa             READ             APIM REST endpoints     │
│  compliance-officer      READ + audit     Purview compliance view │
│  defender-sa             READ             all sources (scanning)  │
│                                                                  │
│  Purview Data Policy → access enforced at storage source level   │
│  Sensitivity labels  → auto-applied; block access if PII         │
│  Column masking      → dynamic data masking in SQL DB / Synapse  │
│  Row-level security  → Synapse Serverless SQL row filters        │
│  Defender for Cloud  → anomaly detection on SQL + Storage        │
│  Private Endpoints   → ADLS, SQL DB, Cosmos DB not public        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Purview Scan\nscheduled every 6h]
    T2[📥 ADLS Event\nnew file in monitored path]
    T3[🔒 Defender Alert\nanomaly detected on source]

    T1 --> SC1[Purview Scanner\ncrawl SQL · ADLS · Cosmos · Synapse]
    T2 --> SC1
    SC1 --> SC2[Data Map update\nnew/changed assets]
    SC2 --> SC3[Lineage auto-captured\nfrom ADF pipelines + Synapse jobs]
    SC3 --> SC4[Sensitivity label propagation\ndownstream assets inherit labels]

    T3 --> DEF[Defender for Cloud\nalert enriched with Purview context]
    DEF --> SIEM[Microsoft Sentinel\nSOC investigation]

    SC4 --> POL[Purview Data Policy evaluation\nblock if label = Restricted]
    POL --> APIM[API Management\nexpose approved assets as REST]
    APIM --> CONS[Consumer app\nREST call returns data in-place]
```

---

## Component Map

| Component | Azure Service | Notes |
|-----------|--------------|-------|
| Business Catalog | Microsoft Purview Data Catalog | Business glossary · data product discovery · lineage graph |
| Technical Metadata | Purview Data Map | Auto-scan schema from ADLS, SQL DB, Cosmos, Synapse, on-prem |
| PII Classification | Purview DLP + sensitivity labels | 100+ built-in classifiers; custom regex; propagates to downstream |
| Zero-ETL Link | Azure Synapse Link for Cosmos DB | Analytical store — columnar, no RU cost; auto-sync |
| Zero-ETL Link | Azure Synapse Link for SQL DB | Change feed to analytical endpoint; no DMS needed |
| Federated Query | Synapse Serverless SQL | OPENROWSET over ADLS Parquet/Delta/CSV; pay per TB scanned |
| API Exposure | Azure API Management | REST data product APIs; OAuth + subscription keys |
| Access Control | Purview Data Policy | Source-enforced; no per-app IAM grants needed |
| Authorization | Azure RBAC + ABAC | Conditions on storage account + Synapse workspace |
| Threat Detection | Microsoft Defender for Cloud | SQL threat detection · anomalous access · misconfiguration |
| Audit | Azure Monitor + Purview Audit | All access events; retention to Log Analytics |
| BI Access | Power BI DirectLake | Zero-copy semantic model on ADLS Delta tables |
| Encryption | Azure Key Vault + CMK | ADLS, SQL DB, Cosmos, Synapse all BYOK |

---

## Comparison vs 6.1 (AWS Data Fabric)

| Dimension | 6.2 Azure Managed | 6.1 AWS Managed |
|-----------|------------------|----------------|
| Business catalog | Microsoft Purview | AWS DataZone |
| Technical catalog | Purview Data Map (unified) | Glue Data Catalog (separate) |
| Zero-ETL operational | Synapse Link (Cosmos + SQL) | Athena Lambda connector |
| Federated query | Synapse Serverless SQL | Amazon Athena |
| PII classification | Purview built-in 100+ classifiers | AWS Macie (S3 only) |
| Access control | Purview Data Policy (source-enforced) | Lake Formation (catalog-enforced) |
| API exposure | Azure API Management built-in | Custom API Gateway + Lambda |
| Security posture | Defender for Cloud unified | AWS Security Hub |

---

## When to Choose This Implementation

✅ Azure is primary cloud; Cosmos DB or Azure SQL are key operational sources
✅ Need zero-ETL analytical access to Cosmos DB (Synapse Link)
✅ Microsoft 365 / Entra ID already governs identity — Purview integrates natively
✅ Power BI DirectLake is the primary BI tool
✅ Compliance requires unified lineage from operational to analytical systems

❌ Cross-cloud data on AWS/GCP is primary → use 6.4 (Multi-Cloud Managed)
❌ On-prem Oracle/SAP is primary source → use 6.6 (Hybrid Managed)
❌ Full OSS, no Azure dependency → use 6.5
