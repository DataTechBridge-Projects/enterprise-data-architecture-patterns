---
layout: default
title: "9.2 — Governed / Compliance-First · AWS OSS on Cloud"
---

# 9.2 — Governed / Compliance-First · AWS OSS on Cloud

**Stack:** S3 · Apache Iceberg · Apache Ranger · OpenMetadata · Debezium · AWS KMS
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Build (OSS governance stack on AWS infra)
**Compliance Targets:** HIPAA · PCI DSS · SOX · GDPR

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[PostgreSQL / MySQL\nOLTP]
        S2[Payment / EHR Systems]
        S3[Files / APIs]
    end

    subgraph INGEST["Ingestion"]
        I1[Debezium\nCDC → Kafka]
        I2[Airbyte\nSaaS / File connectors]
        I3[Kafka\nEvent bus + audit stream]
    end

    subgraph CLASSIFY["Classification"]
        CL1[OpenMetadata\nTag PII / PHI / PAN]
        CL2[Custom Spark Job\nPattern-based classifier]
    end

    subgraph STORAGE["Storage — S3 + Iceberg"]
        Z1[LANDING\ns3://landing/\nSSE-KMS]
        Z2[RAW\ns3://raw/\nIceberg · partitioned]
        Z3[CURATED\ns3://curated/\nMasked · Iceberg]
        Z4[VAULT\ns3://vault/\nPII isolated]
    end

    subgraph GOVERN["Governance — Apache Ranger"]
        R1[Ranger Policies\nRBAC + column masking]
        R2[Ranger Audit\nall access logged]
        R3[Ranger Tag Sync\nfrom OpenMetadata]
    end

    subgraph CATALOG["Catalog — OpenMetadata"]
        CM1[Data Catalog\nschema + lineage]
        CM2[Sensitivity Tags\nPII / PHI / PAN]
        CM3[Glossary\nbusiness terms]
    end

    subgraph AUDIT["Audit"]
        AU1[Kafka Audit Topic\nall data access events]
        AU2[S3 Audit Sink\nimmutable log store]
        AU3[OpenSearch\naudit search + alerting]
    end

    subgraph CONSUME["Consumption"]
        F1[Trino\nSQL on Iceberg]
        F2[Superset\nBI dashboards]
        F3[Jupyter\nData science]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2 --> Z2
    Z2 -->|PII route| Z4
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
        A2[Payment API]
        A3[Files]
    end

    subgraph Ingestion
        B1[Debezium\nCDC]
        B2[Airbyte]
        B3[Kafka]
    end

    subgraph Storage["S3 + Iceberg"]
        C1[🔒 Landing]
        C2[🔒 Raw\nIceberg]
        C3[🔒 Curated\nMasked]
        C4[🔐 Vault\nPII]
    end

    subgraph Governance["Ranger + OpenMetadata"]
        D1[Column Mask\nPolicies]
        D2[Row Filter\nPolicies]
        D3[Tag-Based\nAccess]
    end

    subgraph Consume
        E1[Trino\nSQL]
        E2[Superset\nBI]
        E3[Jupyter\nDS]
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
s3://<company>-governed/
│
├── landing/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── raw format · SSE-KMS · 7-day TTL
│
├── raw/
│   └── {source}/{table}/
│       └── Iceberg table · SSE-KMS (raw-key) · OpenMetadata-tagged
│
├── curated/
│   └── {domain}/{entity}/
│       └── Iceberg table · PII tokenized · SSE-KMS (curated-key)
│
└── vault/
    └── {sensitivity}/{entity}/
        └── Iceberg table · SSE-KMS (vault-key CMK) · Ranger strict policy
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Apache Ranger — Policy Engine                                   │
│                                                                  │
│  Role                Access Level   Zone          Masking        │
│  ─────────────────   ────────────   ───────────   ───────────── │
│  data-engineer       Read+Write     Raw+Curated   PII masked     │
│  data-analyst        Read only      Curated       PII masked     │
│  data-scientist      Read only      Raw+Curated   PII masked     │
│  bi-consumer         Read only      Curated       Full mask      │
│  compliance-officer  Read only      All + Vault   Partial reveal │
│  auditor             Read only      Audit sink    No PII         │
│                                                                  │
│  Column masking  → Ranger masking policies (hash / nullify)     │
│  Row filtering   → Ranger row-filter by region/jurisdiction     │
│  Tag-based       → OpenMetadata sensitivity tags → Ranger sync  │
│  Encryption      → SSE-KMS per zone; vault uses CMK             │
│  Audit           → Ranger Audit → Kafka → S3 immutable sink     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow DAG\ndaily 01:00 UTC]
    T2[📡 Kafka Event\nnew CDC batch ready]

    T1 --> J1[Spark Job\nLanding → Raw\nIceberg write + classify]
    J1 --> J2[Spark Job\nPII routing\nRaw → Vault or Curated]
    J2 --> J3[Spark Job\nRaw → Curated\ntokenize + mask]
    J3 --> J4[OpenMetadata API\nupdate lineage + tags]
    J4 --> J5[Ranger Policy Sync\npropagate tag changes]
    J5 --> N1[Slack Alert\npipeline complete]

    T2 --> J1

    J1 -->|fail| ERR[Airflow Alert\n→ PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | S3 | SSE-KMS; separate CMK per zone |
| Table Format | Apache Iceberg | ACID, time-travel, schema evolution |
| CDC Ingestion | Debezium | Postgres/MySQL → Kafka |
| SaaS / File Ingestion | Airbyte | 300+ connectors; OSS self-hosted |
| Event Bus | Apache Kafka (MSK) | CDC stream + audit event bus |
| Classification | Spark custom job + OpenMetadata | Pattern-based PII/PHI/PAN tagging |
| Access Control | Apache Ranger | Column masking, row filter, tag policies |
| Schema Catalog | OpenMetadata | Lineage, sensitivity tags, glossary |
| Encryption | AWS KMS (CMK) | Per-zone; vault key HSM-backed |
| Audit Trail | Ranger Audit → Kafka → S3 | Immutable; query-level + object-level |
| Audit Search | OpenSearch | Ranger audit events; SIEM-ready |
| Query Engine | Trino | Iceberg connector; Ranger-governed |
| BI | Apache Superset | Connected via Trino |
| Orchestration | Apache Airflow | DAGs for classify, mask, load |
| Monitoring | Prometheus + Grafana | Pipeline + Ranger metrics |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| PII detection | Spark classifier + OpenMetadata tags | GDPR, HIPAA |
| Column masking | Ranger column masking policies | HIPAA, PCI |
| Row filtering | Ranger row filter by jurisdiction | GDPR |
| Encryption at rest | SSE-KMS CMK per zone | PCI DSS, HIPAA |
| Encryption in transit | TLS 1.2+ · bucket policy enforce | PCI DSS |
| Audit trail | Ranger Audit → Kafka → S3 | SOX, HIPAA |
| Lineage | OpenMetadata automated lineage | GDPR right to erasure tracing |
| Data residency | S3 region + Iceberg catalog scoping | GDPR |
| Right to erasure | Iceberg delete + reprocess masked | GDPR |
| Least privilege | Ranger RBAC policies | All |

---

## Comparison vs 9.1 (AWS Managed)

| Dimension | 9.2 AWS OSS | 9.1 AWS Managed |
|-----------|------------|-----------------|
| Access Control | Apache Ranger | Lake Formation |
| PII Detection | Custom Spark + OpenMetadata | AWS Macie |
| Catalog | OpenMetadata | Glue Data Catalog |
| Audit | Ranger → Kafka → S3 | CloudTrail + Config |
| Encryption | KMS CMK (customer-managed) | KMS CMK (AWS-managed) |
| Ops Overhead | High (Ranger, Kafka, Airflow) | Low (fully managed) |
| Cost Model | EC2/MSK for services | Pay-per-use |
| Portability | High (OSS stack portable) | AWS lock-in |
