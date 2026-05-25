---
layout: default
title: "9.3 — Governed / Compliance-First · Azure Fully Managed"
---

# 9.3 — Governed / Compliance-First · Azure Fully Managed

**Stack:** Synapse Analytics · Microsoft Purview · Azure Policy · Key Vault · Microsoft Defender for Data
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Buy (fully managed, no infra to operate)
**Compliance Targets:** GDPR · HIPAA · PCI DSS · ISO 27001

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Azure SQL / Cosmos DB]
        S2[On-Prem SQL Server\nvia Self-Hosted IR]
        S3[SaaS Apps\nDynamics · Salesforce]
        S4[Files / Events\nEvent Hubs]
    end

    subgraph INGEST["Ingestion — ADF"]
        I1[Azure Data Factory\nCopy + Mapping Data Flows]
        I2[Synapse Link\nno-ETL analytical copy]
        I3[Event Hubs Capture\nstreaming to ADLS]
    end

    subgraph CLASSIFY["Classification — Purview"]
        CL1[Purview Scanner\nauto PII / PHI detection]
        CL2[Custom Classification Rules\ndomain-specific patterns]
        CL3[Sensitivity Labels\nMicrosoft Information Protection]
    end

    subgraph STORAGE["Storage — ADLS Gen2"]
        Z1[LANDING\nadls://landing/\nSSE + CMK]
        Z2[RAW\nadls://raw/\nParquet · CMK]
        Z3[CURATED\nadls://curated/\nMasked · CMK]
        Z4[VAULT\nadls://vault/\nPII isolated · CMK]
        SYN[Synapse Dedicated Pool\nStructured governed store]
    end

    subgraph CATALOG["Catalog & Policy — Purview"]
        C1[Purview Data Map\nschema + lineage]
        C2[Purview Collections\naccess boundary by domain]
        C3[RBAC Policies\nReader/Curator/Admin]
        C4[Azure Key Vault\nCMK per zone]
    end

    subgraph AUDIT["Audit & Compliance"]
        A1[Azure Monitor\nall activity logs]
        A2[ADLS Diagnostic Logs\nobject-level audit]
        A3[Synapse Audit Logs\nquery-level]
        A4[Azure Policy\nresource compliance]
        A5[Microsoft Defender\ndata threat detection]
    end

    subgraph CONSUME["Consumption"]
        F1[Synapse Serverless SQL\nanalyst SQL — curated]
        F2[Synapse Dedicated Pool\nBI / complex SQL]
        F3[Power BI\ndashboards — governed]
        F4[Azure Databricks\nML / data science]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2 --> CL3
    CL3 -->|PII| Z4
    CL3 -->|non-PII| Z2 --> Z3
    Z3 --> SYN

    Z2 & Z3 -. register .-> C1
    C1 -. policies .-> C2 --> C3
    C4 -. encrypt .-> Z1 & Z2 & Z3 & Z4

    C3 --> F1 & F2 & F3 & F4

    INGEST & Z1 & Z2 & Z3 --> A1
    Z1 & Z2 & Z3 --> A2
    SYN --> A3
    A1 & A2 & A3 --> A4 --> A5
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Azure SQL)]
        A2[On-Prem SQL\nvia IR]
        A3[SaaS / Files]
    end

    subgraph Ingestion["ADF + Synapse Link"]
        B1[ADF Copy\nActivity]
        B2[Mapping\nData Flows]
    end

    subgraph Classification["Purview Scanner\n+ MIP Labels"]
        C1[PII Detection]
        C2[Sensitivity Labels]
    end

    subgraph Storage["ADLS Gen2 — CMK"]
        D1[🔒 Landing]
        D2[🔒 Raw]
        D3[🔒 Curated\nMasked]
        D4[🔐 Vault\nPII-only]
    end

    subgraph Catalog["Purview\nRBAC + Collections"]
        E1[Column Masking\nSynapse DDM]
        E2[Row Security\nRLS Policies]
        E3[Collection\nAccess Boundary]
    end

    subgraph Consume
        F1[Synapse\nServerless SQL]
        F2[Power BI]
        F3[Databricks]
    end

    A1 --> B1 --> D1
    A2 --> B1 --> D1
    A3 --> B2 --> D1

    D1 --> C1 --> C2
    C2 -->|PII| D4
    C2 -->|non-PII| D2 --> D3

    D2 & D3 -->|register| E3
    E1 & E2 & E3 --> F1 & F2 & F3
```

---

## Zone Design

```
adls://<company>-governed/
│
├── landing/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── raw as-received · CMK encrypted · 7-day TTL
│
├── raw/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── Parquet · CMK (raw-key) · Purview-tagged
│
├── curated/
│   └── {domain}/{entity}/year=YYYY/month=MM/
│       └── PII tokenized/masked · Parquet · CMK (curated-key)
│
└── vault/
    └── {classification}/{entity}/year=YYYY/
        └── raw PII · CMK (vault-key HSM) · Purview strict collection
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Microsoft Purview + Synapse Row-Level Security                  │
│                                                                  │
│  Role                  Access Level   Zone          Masking      │
│  ──────────────────    ────────────   ──────────    ─────────── │
│  compliance-admin      Read+Write     All zones     Full         │
│  data-engineer         Read+Write     Raw+Curated   DDM masked   │
│  data-analyst          Read only      Curated       DDM masked   │
│  bi-consumer           Read only      Curated       DDM masked   │
│  auditor               Read only      Audit logs    No PII       │
│  vault-access          Read only      Vault         Authorized   │
│                                                                  │
│  Dynamic Data Masking → Synapse DDM on PII columns             │
│  Row-Level Security   → Synapse RLS by team/region             │
│  Purview Collections  → namespace-level access boundary        │
│  CMK                  → Azure Key Vault per zone               │
│  Defender for Data    → anomaly detection + threat alerts      │
│  Policy               → deny public access, require CMK        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ ADF Schedule Trigger\ndaily 01:00 UTC]
    T2[📡 Purview Alert\nPII detected in landing]

    T1 --> J1[ADF Pipeline\nLanding → Raw\nclassify + tag]
    J1 --> J2[Synapse Notebook\nPII routing\nRaw → Vault or Curated]
    J2 --> J3[Synapse Mapping Flow\nRaw → Curated\nmask + tokenize]
    J3 --> J4[Purview Scan\nupdate lineage + labels]
    J4 --> J5[Synapse COPY\nCurated → Dedicated Pool]
    J5 --> N1[Teams Alert\npipeline complete]

    T2 --> AL1[Logic App\nQuarantine + notify Security]
    AL1 --> AL2[Defender Incident\nhuman review]

    J1 -->|fail| ERR[Azure Monitor Alert\n→ PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | Azure Service | Notes |
|-----------|--------------|-------|
| Object Storage | ADLS Gen2 | Hierarchical namespace; CMK per container |
| DW Storage | Synapse Dedicated Pool | Columnar; DDM + RLS for compliance |
| Ingestion | Azure Data Factory | Self-hosted IR for on-prem; Mapping Flows for transform |
| No-ETL Analytical Copy | Synapse Link | Cosmos DB + SQL Server analytical copy |
| PII Detection | Microsoft Purview Scanner | Custom + built-in classifiers; MIP labels |
| Schema Catalog | Purview Data Map | Lineage; automated scanning; glossary |
| Access Control | Purview RBAC + Collections | Fine-grained namespace access |
| Column Masking | Synapse DDM | Dynamic Data Masking on sensitive columns |
| Row Security | Synapse RLS | Row-level security policies |
| Encryption | Azure Key Vault (CMK) | Per-zone keys; Premium tier HSM for vault |
| Compliance Policy | Azure Policy | Deny public access, require encryption, audit |
| Threat Detection | Microsoft Defender for SQL | Anomaly + SQL injection detection |
| Audit Trail | Azure Monitor + ADLS Diagnostics | Immutable log export to Log Analytics |
| Ad-hoc Query | Synapse Serverless SQL | Governed via Purview; analysts curated only |
| BI | Power BI | Row-level security from Synapse |
| ML / DS | Azure Databricks | Governed via Purview Unity Catalog integration |
| Orchestration | ADF + Synapse Pipelines | ADF for ingestion; Synapse for transform |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| Data residency | ADLS region lock + Azure Policy | GDPR |
| PII detection | Purview Scanner + MIP labels | GDPR, HIPAA |
| Column masking | Synapse DDM | HIPAA, PCI |
| Row filtering | Synapse RLS | GDPR |
| Encryption at rest | CMK via Key Vault (HSM for vault) | PCI DSS, HIPAA |
| Encryption in transit | TLS 1.2+ enforced | PCI DSS |
| Audit trail | Azure Monitor + Diagnostic Logs | SOX, HIPAA |
| Threat detection | Defender for SQL | PCI DSS |
| Right to erasure | ADLS delete + Synapse TRUNCATE | GDPR |
| Compliance posture | Azure Policy + Security Center | PCI, HIPAA |

---

## Comparison vs 9.4 (Azure OSS)

| Dimension | 9.3 Azure Managed | 9.4 Azure OSS |
|-----------|------------------|---------------|
| Access Control | Purview RBAC + Synapse RLS | Apache Ranger |
| PII Detection | Purview Scanner + MIP | Custom classifiers + OpenMetadata |
| Catalog | Microsoft Purview | Collibra / OpenMetadata |
| Encryption | Azure Key Vault CMK | Azure Key Vault + Ranger encryption zone |
| Ops Overhead | Low (managed) | High (self-managed Ranger) |
| Cost Model | Pay-per-use | VM/AKS for Ranger overhead |
| M365 Integration | Native (MIP labels) | Custom |
