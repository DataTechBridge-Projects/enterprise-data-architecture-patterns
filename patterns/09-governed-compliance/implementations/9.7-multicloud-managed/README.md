---
layout: default
title: "9.7 — Governed / Compliance-First · Multi-Cloud Fully Managed"
---

# 9.7 — Governed / Compliance-First · Multi-Cloud Fully Managed

**Stack:** Snowflake · Collibra · Fivetran · Snowflake Dynamic Data Masking · Snowflake Data Clean Room
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Buy (SaaS-first, multi-cloud portable)
**Compliance Targets:** GDPR · HIPAA · PCI DSS · SOX · CCPA

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources — Multi-Cloud"]
        S1[AWS RDS / Redshift]
        S2[Azure SQL / Synapse]
        S3[GCP Cloud SQL / BigQuery]
        S4[SaaS Apps\nSalesforce · Workday · SAP]
        S5[Files / Events]
    end

    subgraph INGEST["Ingestion — Fivetran"]
        I1[Fivetran Connectors\n300+ sources]
        I2[Fivetran\nCDC + Full Load]
        I3[Snowpipe\nstreaming files → Snowflake]
    end

    subgraph CLASSIFY["Classification — Collibra DQ"]
        CL1[Collibra Data Quality\nPII / PHI scanning]
        CL2[Collibra Business Glossary\nsensitivity classification]
        CL3[Snowflake Tags\npropagated from Collibra]
    end

    subgraph STORAGE["Storage — Snowflake"]
        Z1[RAW Schema\nLanding + raw data]
        Z2[GOVERNED Schema\nMasked views + policies]
        Z3[VAULT Schema\nPII isolated + strict access]
        Z4[CLEAN ROOM\nSnowflake Data Clean Room]
    end

    subgraph GOVERN["Governance — Snowflake + Collibra"]
        G1[Dynamic Data Masking\nPII columns auto-masked]
        G2[Row Access Policies\nby role / region]
        G3[Column-Level Security\ntag-based access]
        G4[Collibra Lineage\nend-to-end data lineage]
    end

    subgraph AUDIT["Audit"]
        A1[Snowflake Account Usage\nquery + access history]
        A2[Collibra Audit\ncatalog change events]
        A3[SIEM Integration\nSplunk / Datadog]
    end

    subgraph CONSUME["Consumption"]
        F1[Snowflake SQL\nanalyst queries — masked views]
        F2[Tableau / Power BI\nBI dashboards]
        F3[dbt Cloud\ntransformations]
        F4[Clean Room\npartner data sharing]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2 --> CL3
    CL3 --> Z2
    Z1 -->|PII fields| Z3

    Z2 & Z3 -. register .-> G4
    G1 & G2 & G3 -. enforce .-> F1 & F2 & F3
    Z3 --> Z4

    Z1 & Z2 --> A1
    G4 --> A2
    A1 & A2 --> A3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Multi-Cloud Sources"]
        A1[(AWS RDS)]
        A2[(Azure SQL)]
        A3[(GCP SQL)]
        A4[SaaS APIs]
    end

    subgraph Ingestion["Fivetran + Snowpipe"]
        B1[Fivetran\nConnectors]
        B2[Snowpipe\nFiles]
    end

    subgraph Classification["Collibra DQ\n+ Snowflake Tags"]
        C1[PII Detection]
        C2[Sensitivity Tags]
    end

    subgraph Storage["Snowflake Schemas"]
        D1[RAW Schema]
        D2[GOVERNED Schema\nMasked Views]
        D3[VAULT Schema\nPII Isolated]
        D4[CLEAN ROOM\nSecure Share]
    end

    subgraph Governance["Snowflake DDM\n+ Row Policies"]
        E1[Dynamic\nData Masking]
        E2[Row Access\nPolicies]
        E3[Column\nTag Security]
    end

    subgraph Consume
        F1[Analysts\nSQL]
        F2[BI Tools]
        F3[dbt Cloud]
    end

    A1 & A2 & A3 & A4 --> B1 --> D1
    B2 --> D1

    D1 --> C1 --> C2
    C2 -->|PII| D3
    C2 -->|non-PII| D2

    D2 --> E1 & E2 & E3
    E1 & E2 & E3 --> F1 & F2 & F3
    D3 --> D4
```

---

## Zone Design

```
Snowflake Account: <company>_governed
│
├── DATABASE: raw_db
│   ├── SCHEMA: {source}_landing   — direct Fivetran load, no transform
│   └── SCHEMA: {source}_raw       — typed, partitioned, no masking
│
├── DATABASE: governed_db
│   ├── SCHEMA: {domain}_curated   — PII columns masked via DDM policy
│   └── SCHEMA: {domain}_views     — analyst-facing masked views
│
├── DATABASE: vault_db
│   └── SCHEMA: {classification}   — raw PII; strict RBAC; no direct query
│       (vault-access role only; Snowflake encryption key isolated)
│
└── DATABASE: clean_room_db
    └── Snowflake Data Clean Room  — partner sharing without PII exposure
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Snowflake RBAC + Dynamic Data Masking + Row Access Policies     │
│                                                                  │
│  Role                  Access Level     Schema        Masking    │
│  ──────────────────    ────────────     ──────────    ───────── │
│  COMPLIANCE_ADMIN      Read+Write       All           None       │
│  DATA_ENGINEER         Read+Write       Raw+Governed  DDM active │
│  DATA_ANALYST          Read only        Governed      DDM active │
│  BI_CONSUMER           Read only        Governed      DDM active │
│  AUDITOR               Read only        Account Usage No PII     │
│  VAULT_ACCESS          Read only        Vault         Authorized │
│  CLEAN_ROOM            Controlled share Clean Room    No raw PII │
│                                                                  │
│  Dynamic Data Masking  → policy per column per role             │
│  Row Access Policies   → filter by jurisdiction column          │
│  Column-Level Security → tag → masking policy binding           │
│  Tri-Secret Sharing    → customer-managed key (Snowflake + HSM) │
│  Network Policy        → IP allowlist per role                  │
│  Audit                 → ACCOUNT_USAGE.QUERY_HISTORY            │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Fivetran Schedule\ncontinuous CDC sync]
    T2[⏰ dbt Cloud Schedule\ndaily transform run]

    T1 --> J1[Fivetran\nSync sources → raw_db]
    J1 --> J2[Collibra DQ Scan\ntag PII in raw_db]
    J2 --> J3[Snowflake Task\nRoute PII → vault_db]
    J3 --> J4[dbt Cloud Run\nRaw → Governed\nmask + conform]
    J4 --> J5[Collibra\nupdate lineage + catalog]
    J5 --> N1[Slack Alert\npipeline complete]

    J1 -->|fail| ERR[Fivetran Alert\n→ PagerDuty]
    J4 -->|fail| ERR2[dbt Cloud Alert\n→ PagerDuty]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Warehouse | Snowflake | Multi-cloud (AWS/Azure/GCP); virtual warehouses |
| Ingestion | Fivetran | 300+ connectors; managed CDC; auto-schema migration |
| File Ingestion | Snowpipe | Streaming files from S3/ADLS/GCS → Snowflake |
| Classification | Collibra Data Quality | PII/PHI scanning; sensitivity tags |
| Schema Catalog | Collibra Data Catalog | End-to-end lineage; business glossary |
| Tag Management | Snowflake Tags + Collibra sync | Tags propagate to DDM policies |
| Column Masking | Snowflake Dynamic Data Masking | Role-based masking expressions |
| Row Access | Snowflake Row Access Policies | Filter by jurisdiction attribute |
| PII Isolation | Snowflake Vault Schema + RBAC | Separate database; tri-secret sharing key |
| Data Clean Room | Snowflake Data Clean Room | Partner analytics without raw PII exchange |
| Encryption | Tri-Secret Sharing (Snowflake + customer CMK) | HSM-backed customer key |
| Audit Trail | Snowflake ACCOUNT_USAGE | QUERY_HISTORY + ACCESS_HISTORY views |
| Compliance Posture | Collibra + Snowflake account usage reports | Cross-cloud compliance dashboard |
| SIEM | Splunk / Datadog integration | Snowflake audit logs shipped |
| Transformation | dbt Cloud | Runs on Snowflake; governed models |
| BI | Tableau / Power BI | Row-level security from Snowflake |
| Orchestration | dbt Cloud + Fivetran triggers | Event-driven + scheduled |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| PII detection | Collibra DQ scanning + Snowflake tags | GDPR, HIPAA |
| Column masking | Snowflake Dynamic Data Masking | HIPAA, PCI |
| Row filtering | Snowflake Row Access Policies | GDPR, CCPA |
| PII isolation | Vault schema + tri-secret sharing | HIPAA, PCI |
| Encryption at rest | Tri-Secret Sharing (customer CMK) | PCI DSS, HIPAA |
| Encryption in transit | TLS 1.2+ mandatory | PCI DSS |
| Audit trail | ACCOUNT_USAGE.QUERY_HISTORY | SOX, HIPAA |
| Data clean room | Snowflake Clean Room | GDPR (no raw PII to partners) |
| Right to erasure | Snowflake DELETE + dbt rerun | GDPR, CCPA |
| Data residency | Snowflake region selection | GDPR |

---

## Comparison vs 9.8 (Multi-Cloud OSS)

| Dimension | 9.7 Multi-Cloud Managed | 9.8 Multi-Cloud OSS |
|-----------|------------------------|---------------------|
| DW / Store | Snowflake | Iceberg + Trino |
| Ingestion | Fivetran (managed) | Airbyte + Kafka (self-managed) |
| Access Control | Snowflake DDM + Row Policies | Apache Ranger |
| Catalog | Collibra (SaaS) | OpenMetadata (OSS) |
| Clean Room | Snowflake native | Custom + clean room tooling |
| Ops Overhead | Very Low | High |
| Cost Model | Snowflake credits + Fivetran/Collibra licenses | Self-hosted infra + OSS |
| Portability | Snowflake multi-cloud (3 clouds) | Full OSS portability |
