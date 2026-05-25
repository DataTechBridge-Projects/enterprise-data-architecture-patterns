---
layout: default
title: "9.1 — Governed / Compliance-First · AWS Fully Managed"
---

# 9.1 — Governed / Compliance-First · AWS Fully Managed

**Stack:** Redshift · Lake Formation · Macie · CloudTrail · AWS KMS · AWS Config
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Buy (fully managed, no infra to operate)
**Compliance Targets:** HIPAA · PCI DSS · SOX · GDPR

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[EHR / OLTP\nRDS · Aurora]
        S2[Payment Systems\nPCI-scoped]
        S3[SaaS / Files\nSalesforce · S3 Upload]
        S4[Audit Logs\nCloudTrail · App Logs]
    end

    subgraph INGEST["Ingestion — Controlled"]
        I1[AWS DMS\nCDC → Encrypted Landing]
        I2[AWS Glue ETL\nClassify + Tag on ingest]
        I3[Kinesis Firehose\nLog aggregation]
    end

    subgraph CLASSIFY["Classification & DLP"]
        CL1[AWS Macie\nAuto PII detection]
        CL2[Glue Custom Classifiers\nDomain-specific patterns]
        CL3[Lake Formation Tags\nSensitivity labels]
    end

    subgraph STORAGE["Storage — Amazon S3 + Redshift"]
        Z1[LANDING\ns3://landing/\nSSE-KMS · short TTL]
        Z2[RAW\ns3://raw/\nParquet · KMS-encrypted]
        Z3[CURATED\ns3://curated/\nMasked · Compliant]
        Z4[VAULT\ns3://vault/\nPII isolated · strict ACL]
        RDW[Redshift\nStructured governed store]
    end

    subgraph CATALOG["Catalog & Policy — Lake Formation"]
        C1[Glue Data Catalog\nSchema + lineage]
        C2[Lake Formation RBAC\nColumn/row security]
        C3[Lake Formation ABAC\nTag-based policies]
        C4[AWS KMS\nKey per classification tier]
    end

    subgraph AUDIT["Audit & Compliance"]
        A1[CloudTrail\nAll API calls]
        A2[S3 Access Logs\nObject-level audit]
        A3[Redshift Audit Logs\nQuery-level audit]
        A4[AWS Config\nResource compliance]
        A5[Security Hub\nCompliance posture]
    end

    subgraph CONSUME["Consumption"]
        F1[Amazon Athena\nAnalyst SQL — curated only]
        F2[Redshift\nBI queries — masked view]
        F3[QuickSight\nDashboards — governed]
        F4[Audit Reports\nCompliance exports]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2
    CL1 & CL2 --> CL3
    CL3 --> Z2
    Z2 -->|PII fields → vault| Z4
    Z2 -->|non-PII| Z3
    Z3 --> RDW

    Z2 & Z3 -. register .-> C1
    C1 -. policies .-> C2
    C2 -. enforce .-> C3
    C4 -. encrypt .-> Z1 & Z2 & Z3 & Z4

    C2 --> F1
    C2 --> F2
    C2 --> F3

    INGEST & Z1 & Z2 & Z3 --> A1
    Z1 & Z2 & Z3 --> A2
    RDW --> A3
    A1 & A2 & A3 --> A4 --> A5 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[Payment Systems]
        A3[SaaS / Files]
    end

    subgraph Ingestion
        B1[AWS DMS\nCDC]
        B2[Glue ETL\n+ Classifier]
    end

    subgraph Classification["Macie + LF Tags"]
        C1[PII Detection]
        C2[Sensitivity Tagging]
    end

    subgraph Storage["S3 Zones — SSE-KMS"]
        D1[🔒 Landing]
        D2[🔒 Raw]
        D3[🔒 Curated\nMasked]
        D4[🔐 Vault\nPII-only]
    end

    subgraph Catalog["Lake Formation\nRBAC + ABAC"]
        E1[Column Masking]
        E2[Row Filtering]
        E3[Tag-Based Access]
    end

    subgraph Consume
        F1[Athena\nAnalysts]
        F2[Redshift\nBI / Finance]
        F3[QuickSight\nExecs]
    end

    A1 --> B1 --> D1
    A2 --> B2 --> D1
    A3 --> B2 --> D1

    D1 --> C1 --> C2
    C2 -->|PII| D4
    C2 -->|non-PII| D2 --> D3

    D2 & D3 -->|register| E1
    E1 & E2 & E3 -->|govern| F1 & F2 & F3
```

---

## Zone Design

```
s3://<company>-governed/
│
├── landing/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── raw as-received · SSE-KMS (landing-key) · 7-day TTL
│
├── raw/
│   └── {source}/{table}/year=YYYY/month=MM/day=DD/
│       └── Parquet · SSE-KMS (raw-key) · Macie-tagged
│
├── curated/
│   └── {domain}/{entity}/year=YYYY/month=MM/
│       └── PII fields tokenized/masked · Parquet · SSE-KMS (curated-key)
│
└── vault/
    └── {classification}/{entity}/year=YYYY/
        └── raw PII fields · SSE-KMS (vault-key) · strict IAM boundary
            Lake Formation row/col policies + separate KMS CMK
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Lake Formation RBAC + ABAC                                      │
│                                                                  │
│  IAM Role           Access Level   Zone          Column Policy   │
│  ─────────────────  ────────────   ───────────   ─────────────  │
│  compliance-admin   Read+Write     All zones     Full access     │
│  data-engineer      Read+Write     Raw+Curated   PII masked      │
│  data-analyst       Read only      Curated       PII masked      │
│  bi-consumer        Read only      Curated       PII masked      │
│  auditor            Read only      Audit logs    No PII          │
│  vault-access       Read only      Vault         Authorized PHI  │
│                                                                  │
│  Column masking  → PII / PHI / PAN via Lake Formation           │
│  Row filtering   → by region / jurisdiction (GDPR residency)    │
│  ABAC tags       → sensitivity:high → requires vault-access role │
│  KMS CMKs        → 1 key per zone; vault key HSM-backed         │
│  Macie           → continuous scanning · alerts on exposure     │
│  CloudTrail      → all s3:GetObject + LF:GetDataAccess logged   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Schedule Trigger\ndaily 01:00 UTC]
    T2[📡 Macie Finding\nPII detected in landing]

    T1 --> J1[Glue Job\nLanding → Raw\nclassify + tag]
    J1 --> J2[Lambda\nRoute PII → Vault\nRoute non-PII → Raw]
    J2 --> J3[Glue Job\nRaw → Curated\nmask + tokenize]
    J3 --> J4[Glue Crawler\nupdate catalog]
    J4 --> J5[Redshift COPY\nCurated → DW]
    J5 --> N1[SNS Alert\npipeline complete]

    T2 --> AL1[Lambda\nQuarantine object\nAlert Security Hub]
    AL1 --> AL2[Step Functions\nHuman review workflow]

    J1 -->|fail| ERR[CloudWatch Alarm\n→ SNS → PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | AWS Service | Notes |
|-----------|-------------|-------|
| Object Storage | S3 | SSE-KMS; separate CMK per zone |
| DW Storage | Redshift | Encrypted at rest; column-level security via LF |
| DB Ingestion | AWS DMS | CDC with VPC; no plaintext transit |
| File / SaaS Ingestion | AWS Glue ETL | Custom classifiers for PHI/PAN patterns |
| PII Detection | AWS Macie | Scans landing zone; auto-tags sensitivity |
| Schema Catalog | Glue Data Catalog | LF-integrated; tags inherited by catalog entries |
| Access Control | Lake Formation RBAC/ABAC | Column masking, row filter, tag-based policies |
| Encryption | AWS KMS (CMK) | Per-zone keys; vault key backed by CloudHSM |
| Audit Trail | CloudTrail + S3 Access Logs | Immutable; shipped to dedicated audit account |
| Resource Compliance | AWS Config | Rules for encryption, public access blocks, KMS rotation |
| Compliance Posture | AWS Security Hub | HIPAA, PCI DSS standards checks |
| Ad-hoc Query | Amazon Athena | Governed via LF; analysts see masked curated only |
| BI Engine | Redshift | Views enforce masking; query logs audited |
| Dashboards | Amazon QuickSight | SPICE; governed by LF permissions |
| Orchestration | AWS Glue Workflows + Step Functions | Step Functions for human-in-loop review on PII alerts |
| Monitoring | CloudWatch + Security Hub | Dashboards per compliance standard |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| Data residency | S3 bucket region lock + Config rule | GDPR |
| PII detection & tagging | Macie + LF tags | GDPR, HIPAA |
| Column masking | Lake Formation column mask | HIPAA, PCI |
| Encryption at rest | SSE-KMS with CMK rotation | PCI DSS, HIPAA |
| Encryption in transit | TLS 1.2+ enforced via bucket policy | PCI DSS |
| Access audit trail | CloudTrail + S3 access logs | SOX, HIPAA |
| Right to erasure | Vault delete + Glue job to reprocess masked curated | GDPR |
| Least privilege | Lake Formation + IAM permission boundaries | All |
| Key management | KMS with annual rotation | PCI DSS |
| Compliance scan | Security Hub + AWS Config rules | PCI, HIPAA |

---

## Comparison vs 9.2 (AWS OSS)

| Dimension | 9.1 AWS Managed | 9.2 AWS OSS |
|-----------|----------------|-------------|
| Access Control | Lake Formation RBAC/ABAC | Apache Ranger |
| PII Detection | AWS Macie | Custom classifiers + OpenMetadata |
| Audit | CloudTrail + Config | Debezium audit + custom sink |
| Encryption | KMS managed | Customer-managed KMS + Ranger encryption zone |
| Ops Overhead | Low (managed) | High (self-managed Ranger) |
| Cost Model | Pay-per-use | EC2/ECS for Ranger + overhead |
| Compliance Certification | AWS BAA / PCI DSS AoC | Customer responsibility |
