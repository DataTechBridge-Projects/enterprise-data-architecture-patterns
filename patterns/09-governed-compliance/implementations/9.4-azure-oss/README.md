---
layout: default
title: "9.4 — Governed / Compliance-First · Azure OSS on Cloud"
---

# 9.4 — Governed / Compliance-First · Azure OSS on Cloud

**Stack:** ADLS Gen2 · Delta Lake · Apache Ranger · Collibra · Azure Key Vault
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Build (OSS governance on Azure infra)
**Compliance Targets:** GDPR · HIPAA · PCI DSS · SOX

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Azure SQL / PostgreSQL]
        S2[On-Prem / Legacy Systems]
        S3[SaaS APIs]
        S4[Event Hubs]
    end

    subgraph INGEST["Ingestion"]
        I1[Debezium on AKS\nCDC → Event Hubs]
        I2[Airbyte on AKS\nSaaS / File connectors]
        I3[Event Hubs\nstream buffer]
    end

    subgraph CLASSIFY["Classification"]
        CL1[Collibra DQ\nPII / PHI tagging]
        CL2[Spark Classifier\ncustom pattern matching]
    end

    subgraph STORAGE["Storage — ADLS + Delta Lake"]
        Z1[LANDING\nadls://landing/\nCMK encrypted]
        Z2[RAW\nadls://raw/\nDelta Lake · CMK]
        Z3[CURATED\nadls://curated/\nMasked · Delta Lake]
        Z4[VAULT\nadls://vault/\nPII isolated · CMK]
    end

    subgraph GOVERN["Governance — Apache Ranger"]
        R1[Ranger Policies\nRBAC + column masking]
        R2[Ranger Audit\nall access events]
        R3[Ranger Tag Sync\nfrom Collibra]
    end

    subgraph CATALOG["Catalog — Collibra"]
        CM1[Data Catalog\nschema + lineage]
        CM2[Sensitivity Tags\nPII / PHI / PAN]
        CM3[Data Stewardship\napproval workflows]
    end

    subgraph AUDIT["Audit"]
        AU1[Event Hubs Audit Topic\nall data access events]
        AU2[ADLS Audit Sink\nimmutable log store]
        AU3[Azure Monitor\ncentralized alerting]
    end

    subgraph CONSUME["Consumption"]
        F1[Trino on AKS\nSQL on Delta Lake]
        F2[Apache Superset\nBI dashboards]
        F3[Databricks\nML / data science]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2
    CL1 & CL2 --> Z2
    Z2 -->|PII| Z4
    Z2 -->|clean| Z3

    Z2 & Z3 -. register .-> CM1
    CM2 -. sync tags .-> R3 --> R1
    R1 -. enforce .-> F1 & F2 & F3
    R2 --> AU1 --> AU2 --> AU3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(PostgreSQL)]
        A2[On-Prem]
        A3[SaaS]
    end

    subgraph Ingestion
        B1[Debezium CDC]
        B2[Airbyte]
        B3[Event Hubs]
    end

    subgraph Storage["ADLS + Delta Lake"]
        C1[🔒 Landing]
        C2[🔒 Raw\nDelta]
        C3[🔒 Curated\nMasked]
        C4[🔐 Vault\nPII]
    end

    subgraph Governance["Ranger + Collibra"]
        D1[Column Mask]
        D2[Row Filter]
        D3[Tag Policy]
    end

    subgraph Consume
        E1[Trino\nSQL]
        E2[Superset\nBI]
        E3[Databricks\nML]
    end

    A1 --> B1 --> B3 --> C1
    A2 --> B2 --> C1
    A3 --> B2 --> C1

    C1 -->|classify| C2
    C2 -->|PII| C4
    C2 -->|masked| C3

    C2 & C3 --> D1 & D2 & D3
    D1 & D2 & D3 --> E1 & E2 & E3
```

---

## Zone Design

```
adls://<company>-governed/
│
├── landing/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── raw format · CMK · 7-day TTL
│
├── raw/
│   └── {source}/{table}/
│       └── Delta Lake table · CMK (raw-key) · Collibra-tagged
│
├── curated/
│   └── {domain}/{entity}/
│       └── Delta Lake table · PII tokenized · CMK (curated-key)
│
└── vault/
    └── {sensitivity}/{entity}/
        └── Delta Lake table · CMK (vault-key HSM) · Ranger strict policy
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Apache Ranger — Policy Engine                                   │
│                                                                  │
│  Role                Access Level   Zone          Masking        │
│  ─────────────────   ────────────   ──────────    ───────────── │
│  data-engineer       Read+Write     Raw+Curated   PII masked     │
│  data-analyst        Read only      Curated       PII masked     │
│  data-scientist      Read only      Raw+Curated   PII masked     │
│  bi-consumer         Read only      Curated       Full mask      │
│  compliance-officer  Read only      All + Vault   Partial        │
│  auditor             Read only      Audit sink    No PII         │
│                                                                  │
│  Column masking  → Ranger hash / nullify masking policies       │
│  Row filtering   → Ranger row-filter by jurisdiction            │
│  Tag-based       → Collibra sensitivity tags → Ranger sync      │
│  Encryption      → Azure Key Vault CMK per zone                 │
│  Audit           → Ranger Audit → Event Hubs → ADLS sink        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow DAG\ndaily 01:00 UTC]
    T2[📡 Event Hub Event\nnew CDC batch]

    T1 --> J1[Databricks Job\nLanding → Raw\nDelta write + classify]
    J1 --> J2[Databricks Job\nPII routing\nRaw → Vault or Curated]
    J2 --> J3[Databricks Job\nRaw → Curated\ntokenize + mask]
    J3 --> J4[Collibra API\nupdate lineage + tags]
    J4 --> J5[Ranger Policy Sync\npropagate tag changes]
    J5 --> N1[Teams Alert\npipeline complete]

    T2 --> J1

    J1 -->|fail| ERR[Airflow Alert\n→ PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | ADLS Gen2 | Hierarchical namespace; CMK per container |
| Table Format | Delta Lake | ACID, time-travel; native Databricks support |
| CDC Ingestion | Debezium on AKS | PostgreSQL/SQL Server → Event Hubs |
| SaaS / File Ingestion | Airbyte on AKS | OSS connectors; self-managed |
| Event Bus | Azure Event Hubs | Kafka-compatible; CDC + audit stream |
| Classification | Spark on Databricks + Collibra DQ | Pattern-based PII/PHI/PAN |
| Access Control | Apache Ranger on AKS | Column masking, row filter, tag policies |
| Schema Catalog | Collibra Data Catalog | Lineage, sensitivity tags, stewardship |
| Encryption | Azure Key Vault (CMK) | Per-zone keys; Premium HSM for vault |
| Audit Trail | Ranger Audit → Event Hubs → ADLS | Immutable; query-level + object-level |
| Audit Monitoring | Azure Monitor | Centralized alerting; SIEM integration |
| Query Engine | Trino on AKS | Delta Lake connector; Ranger-governed |
| BI | Apache Superset | Connected via Trino |
| ML / DS | Azure Databricks | Delta Lake native; Ranger governance |
| Orchestration | Apache Airflow on AKS | DAGs for classify, mask, sync |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| PII detection | Spark classifier + Collibra DQ | GDPR, HIPAA |
| Column masking | Ranger column masking policies | HIPAA, PCI |
| Row filtering | Ranger row filter by jurisdiction | GDPR |
| Encryption at rest | CMK per zone (HSM for vault) | PCI DSS, HIPAA |
| Encryption in transit | TLS 1.2+ | PCI DSS |
| Audit trail | Ranger Audit → Event Hubs → ADLS | SOX, HIPAA |
| Lineage | Collibra automated lineage | GDPR |
| Data residency | ADLS region + Ranger scoping | GDPR |
| Right to erasure | Delta Lake DELETE + reprocess | GDPR |
| Stewardship | Collibra approval workflows | Internal governance |

---

## Comparison vs 9.3 (Azure Managed)

| Dimension | 9.4 Azure OSS | 9.3 Azure Managed |
|-----------|--------------|-------------------|
| Access Control | Apache Ranger | Purview RBAC + Synapse RLS |
| Catalog | Collibra | Microsoft Purview |
| Table Format | Delta Lake (OSS) | Parquet + Synapse |
| Portability | High | Azure lock-in |
| Ops Overhead | High (Ranger, Airflow on AKS) | Low |
| Cost Model | AKS cluster + license | Pay-per-use |
| M365 Integration | None | Native MIP labels |
