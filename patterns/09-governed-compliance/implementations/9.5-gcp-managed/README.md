---
layout: default
title: "9.5 — Governed / Compliance-First · GCP Fully Managed"
---

# 9.5 — Governed / Compliance-First · GCP Fully Managed

**Stack:** BigQuery · Data Catalog · Cloud DLP API · Cloud KMS · Dataplex · VPC Service Controls
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Buy (fully managed, no infra to operate)
**Compliance Targets:** GDPR · HIPAA · PCI DSS · FedRAMP

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Cloud SQL / Spanner]
        S2[On-Prem via\nDatastream CDC]
        S3[SaaS / APIs]
        S4[Pub/Sub Events]
    end

    subgraph INGEST["Ingestion"]
        I1[Datastream\nCDC → GCS]
        I2[Cloud Data Fusion\nSaaS + File ingestion]
        I3[Pub/Sub + Dataflow\nstream to GCS]
    end

    subgraph CLASSIFY["Classification — DLP API"]
        CL1[Cloud DLP\nauto PII / PHI detection]
        CL2[Custom InfoTypes\ndomain-specific patterns]
        CL3[Data Catalog Tags\nsensitivity labels]
    end

    subgraph STORAGE["Storage — GCS + BigQuery"]
        Z1[LANDING\ngcs://landing/\nCMEK encrypted]
        Z2[RAW\ngcs://raw/\nParquet · CMEK]
        Z3[CURATED\ngcs://curated/\nMasked · CMEK]
        Z4[VAULT\ngcs://vault/\nPII isolated · CMEK]
        BQ[BigQuery Datasets\nStructured governed store]
    end

    subgraph CATALOG["Catalog & Policy — Data Catalog + Dataplex"]
        C1[Data Catalog\nschema + lineage]
        C2[Dataplex\ndata zones + policies]
        C3[BigQuery Column Policy\nmasking + fine-grained access]
        C4[Cloud KMS CMEK\nper zone]
    end

    subgraph AUDIT["Audit & Compliance"]
        A1[Cloud Audit Logs\nall data access]
        A2[BigQuery Audit Logs\nquery-level]
        A3[Security Command Center\ncompliance posture]
        A4[Access Transparency\nGoogle admin access log]
    end

    subgraph CONSUME["Consumption"]
        F1[BigQuery\nanalyst SQL — curated]
        F2[Looker\nBI dashboards — governed]
        F3[Vertex AI\nML / data science]
        F4[Connected Sheets\nbusiness users]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2 --> CL3
    CL3 -->|PII| Z4
    CL3 -->|non-PII| Z2 --> Z3
    Z3 --> BQ

    Z2 & Z3 -. register .-> C1
    C1 -. policies .-> C2 --> C3
    C4 -. encrypt .-> Z1 & Z2 & Z3 & Z4

    C3 --> F1 & F2 & F3 & F4

    INGEST & BQ & Z1 & Z2 & Z3 --> A1
    BQ --> A2
    A1 & A2 --> A3 --> A4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Cloud SQL)]
        A2[On-Prem\nDatastream]
        A3[SaaS / Files]
    end

    subgraph Ingestion["Datastream + Data Fusion"]
        B1[Datastream\nCDC]
        B2[Data Fusion\nBatch]
        B3[Dataflow\nStream]
    end

    subgraph Classification["Cloud DLP + Data Catalog"]
        C1[PII Detection]
        C2[Sensitivity Tags]
    end

    subgraph Storage["GCS — CMEK"]
        D1[🔒 Landing]
        D2[🔒 Raw]
        D3[🔒 Curated\nMasked]
        D4[🔐 Vault\nPII-only]
    end

    subgraph Catalog["Dataplex + BQ\nColumn Policy"]
        E1[Column Masking\nBQ Policy Tags]
        E2[Row Access\nBQ Row Policies]
        E3[Zone Policy\nDataplex]
    end

    subgraph Consume
        F1[BigQuery\nSQL]
        F2[Looker\nBI]
        F3[Vertex AI\nML]
    end

    A1 --> B1 --> D1
    A2 --> B1 --> D1
    A3 --> B2 --> D1

    D1 --> C1 --> C2
    C2 -->|PII| D4
    C2 -->|non-PII| D2 --> D3

    D2 & D3 -->|BQ external tables| E1 & E2 & E3
    E1 & E2 & E3 --> F1 & F2 & F3
```

---

## Zone Design

```
gcs://<company>-governed/
│
├── landing/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── raw as-received · CMEK · 7-day lifecycle
│
├── raw/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── Parquet · CMEK (raw-key) · DLP-tagged
│
├── curated/
│   └── {domain}/{entity}/year=YYYY/month=MM/
│       └── PII de-identified · Parquet · CMEK (curated-key)
│
└── vault/
    └── {classification}/{entity}/year=YYYY/
        └── raw PII · CMEK (vault-key HSM) · Dataplex strict zone
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  BigQuery Column Policy Tags + IAM + Dataplex Zones              │
│                                                                  │
│  IAM Role / Group      Access Level   Zone          BQ Masking   │
│  ──────────────────    ────────────   ──────────    ──────────── │
│  compliance-admin      Read+Write     All zones     Full access  │
│  data-engineer         Read+Write     Raw+Curated   Policy Tag   │
│  data-analyst          Read only      Curated       Full mask    │
│  bi-consumer           Read only      Curated       Full mask    │
│  auditor               Read only      Audit logs    No PII       │
│  vault-access          Read only      Vault         Authorized   │
│                                                                  │
│  BQ Column Policy Tags → masking rules on PII/PHI columns      │
│  BQ Row Access Policies → filter by jurisdiction/region        │
│  VPC Service Controls  → perimeter around BQ + GCS + KMS       │
│  CMEK                  → Cloud KMS per zone; HSM for vault      │
│  Cloud DLP             → continuous scanning + de-identification│
│  Cloud Audit Logs      → DATA_READ events; immutable in BQ      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Cloud Scheduler\ndaily 01:00 UTC]
    T2[📡 DLP Finding\nPII in landing]

    T1 --> J1[Dataflow Job\nLanding → Raw\nclassify + tag]
    J1 --> J2[Cloud Function\nRoute PII → Vault\nRoute clean → Raw]
    J2 --> J3[Dataflow Job\nRaw → Curated\nde-identify + mask]
    J3 --> J4[BigQuery Load Job\nCurated → BQ Dataset]
    J4 --> J5[Data Catalog API\nupdate tags + lineage]
    J5 --> N1[Pub/Sub → Teams\npipeline complete]

    T2 --> AL1[Cloud Function\nQuarantine object\nAlert SCC]
    AL1 --> AL2[Security Command Center\nhuman review workflow]

    J1 -->|fail| ERR[Cloud Monitoring Alert\n→ PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | GCP Service | Notes |
|-----------|------------|-------|
| Object Storage | GCS | CMEK per bucket; uniform bucket-level access |
| DW Storage | BigQuery | Column Policy Tags; row access policies |
| CDC Ingestion | Datastream | Direct CDC from PostgreSQL/MySQL/Oracle |
| File / SaaS Ingestion | Cloud Data Fusion | GUI-driven pipelines; 150+ connectors |
| Stream Ingestion | Pub/Sub + Dataflow | Sub-second ingest to GCS |
| PII Detection | Cloud DLP API | 100+ built-in infoTypes; custom infoTypes |
| De-identification | Cloud DLP API | Tokenization, pseudonymization, masking |
| Schema Catalog | Google Data Catalog | Auto-discovery; lineage; sensitivity tags |
| Data Zones | Dataplex | Logical zones with policy enforcement |
| Column Masking | BigQuery Column Policy Tags | Masking rules on tagged columns |
| Row Access | BigQuery Row Access Policies | Filter by group/attribute |
| Encryption | Cloud KMS CMEK | Per-zone keys; Cloud HSM for vault |
| Network Perimeter | VPC Service Controls | Prevent data exfiltration |
| Audit Trail | Cloud Audit Logs | DATA_READ/WRITE; immutable export to BQ |
| Compliance Posture | Security Command Center | Posture checks; threat detection |
| Access Transparency | Access Transparency Logs | Google SRE access audit |
| BI | Looker | LookML governed; BQ policy tags respected |
| ML | Vertex AI | Governed datasets from curated BQ |
| Orchestration | Cloud Composer (Airflow) | DAGs for classify, mask, BQ load |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| Data residency | GCS region lock + Organization Policy | GDPR |
| PII detection | Cloud DLP built-in + custom infoTypes | GDPR, HIPAA |
| Column masking | BQ Column Policy Tags | HIPAA, PCI |
| Row filtering | BQ Row Access Policies | GDPR |
| De-identification | Cloud DLP tokenization | HIPAA Safe Harbor |
| Encryption at rest | CMEK (Cloud KMS / HSM) | PCI DSS, HIPAA |
| Network perimeter | VPC Service Controls | PCI DSS |
| Audit trail | Cloud Audit Logs → BQ | SOX, HIPAA |
| Compliance posture | Security Command Center | PCI, HIPAA |
| Right to erasure | GCS delete + BQ table expiry | GDPR |

---

## Comparison vs 9.6 (GCP OSS)

| Dimension | 9.5 GCP Managed | 9.6 GCP OSS |
|-----------|----------------|-------------|
| Access Control | BQ Column Policy + Dataplex | Apache Ranger |
| PII Detection | Cloud DLP API | Custom + OpenMetadata |
| Catalog | Data Catalog + Dataplex | OpenMetadata |
| Encryption | Cloud KMS CMEK | Cloud KMS + Ranger encryption zone |
| Network Perimeter | VPC Service Controls | Custom VPC + Ranger |
| Ops Overhead | Low (fully managed) | High (Ranger on GKE) |
| Cost Model | Pay-per-query | GKE cluster + overhead |
| FedRAMP | GCP FedRAMP High | Customer responsibility |
