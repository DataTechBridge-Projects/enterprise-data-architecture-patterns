---
layout: default
title: "9.6 — Governed / Compliance-First · GCP OSS on Cloud"
---

# 9.6 — Governed / Compliance-First · GCP OSS on Cloud

**Stack:** GCS · Apache Iceberg · Apache Ranger · OpenMetadata · Cloud KMS
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Build (OSS governance on GCP infra)
**Compliance Targets:** GDPR · HIPAA · PCI DSS

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Cloud SQL / PostgreSQL]
        S2[On-Prem / Legacy]
        S3[SaaS / Files]
        S4[Pub/Sub Events]
    end

    subgraph INGEST["Ingestion"]
        I1[Debezium on GKE\nCDC → Pub/Sub]
        I2[Airbyte on GKE\nSaaS / File connectors]
        I3[Pub/Sub\nevent bus]
    end

    subgraph CLASSIFY["Classification"]
        CL1[OpenMetadata\nTag PII / PHI / PAN]
        CL2[Spark Classifier\ncustom patterns]
    end

    subgraph STORAGE["Storage — GCS + Iceberg"]
        Z1[LANDING\ngcs://landing/\nCMEK]
        Z2[RAW\ngcs://raw/\nIceberg · CMEK]
        Z3[CURATED\ngcs://curated/\nMasked · Iceberg]
        Z4[VAULT\ngcs://vault/\nPII isolated · CMEK]
    end

    subgraph GOVERN["Governance — Apache Ranger on GKE"]
        R1[Ranger Policies\nRBAC + column masking]
        R2[Ranger Audit\nall access events]
        R3[Ranger Tag Sync\nfrom OpenMetadata]
    end

    subgraph CATALOG["Catalog — OpenMetadata"]
        CM1[Data Catalog\nschema + lineage]
        CM2[Sensitivity Tags\nPII / PHI / PAN]
        CM3[Glossary + Lineage\nbusiness context]
    end

    subgraph AUDIT["Audit"]
        AU1[Pub/Sub Audit Topic\nall data access events]
        AU2[GCS Audit Sink\nimmutable]
        AU3[Cloud Monitoring\ncentralized alerting]
    end

    subgraph CONSUME["Consumption"]
        F1[Trino on GKE\nSQL on Iceberg]
        F2[Apache Superset\nBI dashboards]
        F3[Vertex AI\nML / data science]
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
        A1[(Cloud SQL)]
        A2[On-Prem]
        A3[SaaS]
    end

    subgraph Ingestion
        B1[Debezium CDC]
        B2[Airbyte]
        B3[Pub/Sub]
    end

    subgraph Storage["GCS + Iceberg"]
        C1[🔒 Landing]
        C2[🔒 Raw\nIceberg]
        C3[🔒 Curated\nMasked]
        C4[🔐 Vault\nPII]
    end

    subgraph Governance["Ranger + OpenMetadata"]
        D1[Column Mask]
        D2[Row Filter]
        D3[Tag Policy]
    end

    subgraph Consume
        E1[Trino\nSQL]
        E2[Superset\nBI]
        E3[Vertex AI\nML]
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
gcs://<company>-governed/
│
├── landing/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── raw format · CMEK · 7-day lifecycle
│
├── raw/
│   └── {source}/{table}/
│       └── Iceberg table · CMEK (raw-key) · OpenMetadata-tagged
│
├── curated/
│   └── {domain}/{entity}/
│       └── Iceberg table · PII tokenized · CMEK (curated-key)
│
└── vault/
    └── {sensitivity}/{entity}/
        └── Iceberg table · CMEK (vault-key HSM) · Ranger strict policy
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Apache Ranger — Policy Engine (on GKE)                          │
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
│  Tag-based       → OpenMetadata sensitivity tags → Ranger sync  │
│  Encryption      → Cloud KMS CMEK per zone; HSM for vault       │
│  Audit           → Ranger Audit → Pub/Sub → GCS immutable       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Cloud Composer DAG\ndaily 01:00 UTC]
    T2[📡 Pub/Sub Event\nnew CDC batch]

    T1 --> J1[Spark on Dataproc\nLanding → Raw\nIceberg write + classify]
    J1 --> J2[Spark Job\nPII routing\nRaw → Vault or Curated]
    J2 --> J3[Spark Job\nRaw → Curated\ntokenize + mask]
    J3 --> J4[OpenMetadata API\nupdate lineage + tags]
    J4 --> J5[Ranger Policy Sync\npropagate tag changes]
    J5 --> N1[Pub/Sub → Slack\npipeline complete]

    T2 --> J1

    J1 -->|fail| ERR[Cloud Monitoring Alert\n→ PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | GCS | CMEK per bucket; uniform access |
| Table Format | Apache Iceberg | ACID, time-travel; Spark + Trino read |
| CDC Ingestion | Debezium on GKE | PostgreSQL/MySQL → Pub/Sub |
| SaaS / File Ingestion | Airbyte on GKE | OSS connectors; self-managed |
| Event Bus | Google Pub/Sub | CDC stream + audit event bus |
| Classification | Spark on Dataproc + OpenMetadata | Pattern-based PII/PHI/PAN |
| Access Control | Apache Ranger on GKE | Column masking, row filter, tag policies |
| Schema Catalog | OpenMetadata | Lineage, sensitivity tags, glossary |
| Encryption | Cloud KMS CMEK | Per-zone keys; Cloud HSM for vault |
| Audit Trail | Ranger Audit → Pub/Sub → GCS | Immutable; query + object level |
| Audit Monitoring | Cloud Monitoring | Alerts + dashboards |
| Query Engine | Trino on GKE | Iceberg connector; Ranger-governed |
| BI | Apache Superset | Connected via Trino |
| ML | Vertex AI | Governed curated zone datasets |
| Orchestration | Cloud Composer (Airflow) | DAGs for classify, mask, load |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| PII detection | Spark classifier + OpenMetadata tags | GDPR, HIPAA |
| Column masking | Ranger column masking policies | HIPAA, PCI |
| Row filtering | Ranger row filter by jurisdiction | GDPR |
| Encryption at rest | CMEK per zone (Cloud HSM for vault) | PCI DSS, HIPAA |
| Encryption in transit | TLS 1.2+ | PCI DSS |
| Audit trail | Ranger Audit → Pub/Sub → GCS | SOX, HIPAA |
| Lineage | OpenMetadata automated lineage | GDPR |
| Data residency | GCS region + Ranger scoping | GDPR |
| Right to erasure | Iceberg DELETE + reprocess | GDPR |
| Least privilege | Ranger RBAC policies | All |

---

## Comparison vs 9.5 (GCP Managed)

| Dimension | 9.6 GCP OSS | 9.5 GCP Managed |
|-----------|------------|-----------------|
| Access Control | Apache Ranger | BQ Column Policy + Dataplex |
| PII Detection | Custom + OpenMetadata | Cloud DLP API |
| Catalog | OpenMetadata | Google Data Catalog |
| Table Format | Iceberg (OSS) | Parquet + BQ native |
| Portability | High (OSS portable) | GCP lock-in |
| Ops Overhead | High (GKE clusters) | Low (fully managed) |
| Cost Model | GKE + Dataproc | Pay-per-query |
| FedRAMP Support | Customer responsibility | GCP FedRAMP High |
