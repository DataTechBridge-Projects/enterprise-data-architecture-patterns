---
layout: default
title: "9.10 — Governed / Compliance-First · Hybrid OSS Self-Hosted"
---

# 9.10 — Governed / Compliance-First · Hybrid OSS Self-Hosted

**Stack:** Apache Atlas · Apache Ranger · HashiCorp Vault · OpenMetadata · Audit Sinks
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Full Build (all components self-hosted on-prem + cloud)
**Compliance Targets:** GDPR · HIPAA · PCI DSS · SOX

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources — Hybrid"]
        S1[On-Prem RDBMS\nPostgreSQL · Oracle · MySQL]
        S2[On-Prem Files\nNFS · HDFS]
        S3[Cloud Storage\nS3 · ADLS · GCS]
        S4[SaaS APIs]
    end

    subgraph INGEST["Ingestion — OSS"]
        I1[Debezium\nCDC → Kafka]
        I2[Airbyte\nSaaS + file connectors]
        I3[Kafka\ncross-environment event bus]
        I4[NiFi\ncomplex on-prem routing]
    end

    subgraph CLASSIFY["Classification"]
        CL1[OpenMetadata\nPII / PHI / PAN tagging]
        CL2[Spark Classifier\ncustom patterns]
        CL3[Apache Atlas\nsensitivity classifications]
    end

    subgraph STORAGE["Storage — Hybrid"]
        Z1[ON-PREM LANDING\nHDFS / NAS\nVault-encrypted]
        Z2[ON-PREM RAW\nHDFS + Iceberg\nVault-encrypted]
        Z3[CLOUD CURATED\nObject Store + Iceberg\nKMS-encrypted · Masked]
        Z4[VAULT ZONE\nOn-prem dedicated cluster\nHSM-backed Vault]
    end

    subgraph GOVERN["Governance — Ranger + Atlas"]
        R1[Apache Ranger\nRBAC + column masking]
        R2[Apache Ranger Audit\nall access logged]
        R3[Apache Atlas\nlineage + classification sync]
    end

    subgraph CATALOG["Catalog — OpenMetadata + Atlas"]
        CM1[OpenMetadata\nschema + lineage + tags]
        CM2[Apache Atlas\nHadoop / Spark lineage]
        CM3[Glossary\nbusiness terms + stewardship]
    end

    subgraph SECRETS["Secrets — HashiCorp Vault"]
        HV1[Dynamic Secrets\nDB creds per session]
        HV2[Transit Engine\ntokenization / FPE]
        HV3[Key Management\nper-zone encryption keys]
        HV4[PKI Engine\ncert lifecycle]
    end

    subgraph AUDIT["Audit Sinks"]
        AU1[Kafka Audit Topic\nall access + policy events]
        AU2[HDFS / S3 Audit Sink\nimmutable log store]
        AU3[OpenSearch\naudit search + alerting]
        AU4[SIEM\nSplunk / Elastic SIEM]
    end

    subgraph CONSUME["Consumption"]
        F1[Trino\ncross-env SQL on Iceberg]
        F2[Apache Superset\nBI dashboards]
        F3[Spark / Jupyter\nML · data science]
        F4[Custom Compliance Reports\nscheduled Spark jobs]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2 --> CL3
    CL3 -->|PII| Z4
    CL3 -->|clean| Z2
    Z2 -->|replicate to cloud| Z3

    Z2 & Z3 -. register .-> CM1
    CM1 -. sync .-> CM2
    CM2 -. tag .-> R3 --> R1
    HV1 & HV2 & HV3 -. secrets + keys .-> Z1 & Z2 & Z3 & Z4

    R1 -. enforce .-> F1 & F2 & F3 & F4
    R2 --> AU1 --> AU2 --> AU3 --> AU4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["On-Prem + Cloud"]
        A1[(PostgreSQL\nOn-Prem)]
        A2[HDFS Files]
        A3[Cloud / SaaS]
    end

    subgraph Ingestion
        B1[Debezium CDC]
        B2[NiFi On-Prem]
        B3[Airbyte Cloud]
        B4[Kafka]
    end

    subgraph Storage["HDFS + Cloud Iceberg"]
        C1[🔒 On-Prem Landing\nVault-enc]
        C2[🔒 On-Prem Raw\nIceberg]
        C3[🔒 Cloud Curated\nMasked Iceberg]
        C4[🔐 On-Prem Vault\nHSM]
    end

    subgraph Governance["Ranger + Atlas\n+ HashiCorp Vault"]
        D1[Column Mask\nRanger]
        D2[Row Filter\nRanger]
        D3[Tokenize\nVault Transit]
    end

    subgraph Consume
        E1[Trino\nSQL]
        E2[Superset\nBI]
        E3[Spark\nML]
    end

    A1 --> B1 --> B4 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1

    C1 -->|classify| C2
    C2 -->|PII| C4
    C2 -->|vault tokenize| C3

    C2 & C3 --> D1 & D2 & D3
    D1 & D2 & D3 --> E1 & E2 & E3
```

---

## Zone Design

```
Hybrid Zone Layout:
│
├── ON-PREM (Kubernetes cluster or bare-metal)
│   ├── landing/    → HDFS / NAS · Vault Transit encrypted · 7-day
│   ├── raw/        → Iceberg on HDFS · Vault KMS key · Atlas-tagged
│   └── vault/      → Dedicated namespace · HSM-backed Vault cluster
│                      on-prem network isolation · Ranger strict policy
│
└── CLOUD  (S3 / ADLS / GCS — whichever primary cloud)
    ├── curated/    → Iceberg · SSE with Vault-managed key
    │   └── {domain}/{entity}/  → PII tokenized via Vault Transit
    └── audit-sink/ → immutable · separate account/subscription
        └── Kafka connector → object store · WORM policy
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Apache Ranger + HashiCorp Vault + Apache Atlas                  │
│                                                                  │
│  Role                Access Level   Zone          Protection     │
│  ─────────────────   ────────────   ──────────    ─────────────│
│  data-engineer       Read+Write     Raw+Curated   Ranger masked  │
│  data-analyst        Read only      Curated       Ranger masked  │
│  data-scientist      Read only      Raw+Curated   Ranger masked  │
│  bi-consumer         Read only      Curated       Full mask      │
│  compliance-officer  Read only      All+Vault     Partial reveal │
│  auditor             Read only      Audit sink    No PII         │
│                                                                  │
│  Column masking  → Ranger masking policies (hash/nullify/FPE)  │
│  Row filtering   → Ranger row-filter by jurisdiction attribute  │
│  Tokenization    → Vault Transit Engine (format-preserving)     │
│  Dynamic secrets → Vault issues per-session DB credentials      │
│  Key mgmt        → Vault KMS + on-prem HSM (Thales/SafeNet)    │
│  PKI             → Vault PKI engine for service mTLS certs     │
│  Audit           → Ranger + Vault audit → Kafka → WORM sink    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow DAG on K8s\ndaily 01:00 UTC]
    T2[📡 Kafka Event\nnew CDC batch]

    T1 --> J1[Spark on K8s\nLanding → Raw\nIceberg write + classify]
    J1 --> J2[Vault API Call\nTokenize PII → Vault zone\nclean fields → Raw]
    J2 --> J3[Spark on K8s\nRaw → Cloud Curated\ntokenized Iceberg write]
    J3 --> J4[OpenMetadata + Atlas API\nupdate lineage + tags]
    J4 --> J5[Ranger Policy Sync\ntag → policy propagation]
    J5 --> N1[Kafka → Slack\npipeline complete]

    T2 --> J1

    J1 -->|fail| ERR[Airflow Alert\n→ PagerDuty]
    J3 -->|fail| ERR
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| On-Prem Storage | HDFS / NetApp NAS | Ranger-governed; Vault-encrypted |
| Cloud Storage | S3 / ADLS / GCS | SSE with Vault-managed CMK |
| Table Format | Apache Iceberg | Portable; Spark + Trino; ACID |
| CDC Ingestion | Debezium | PostgreSQL/MySQL/Oracle → Kafka |
| On-Prem Routing | Apache NiFi | Complex on-prem data routing + provenance |
| SaaS Ingestion | Airbyte on K8s | 300+ connectors; self-managed |
| Event Bus | Apache Kafka on K8s | Cross-environment; CDC + audit |
| Classification | Spark + OpenMetadata | Custom PII/PHI/PAN classifier |
| Access Control | Apache Ranger | Column masking, row filter, HDFS + Trino plugins |
| Metadata Catalog | Apache Atlas | Hadoop/Spark native lineage |
| Schema Catalog | OpenMetadata | Full catalog; Atlas sync; glossary |
| Secrets & Keys | HashiCorp Vault | Dynamic secrets, Transit tokenization, KMS, PKI |
| On-Prem HSM | Thales Luna / SafeNet | Vault HSM auto-unseal; vault zone key |
| Audit Trail | Ranger Audit + Vault Audit → Kafka → WORM | Immutable; cross-env consistent |
| Audit Search | OpenSearch | Ranger + Vault events + SIEM feed |
| SIEM | Splunk / Elastic SIEM | Audit events + threat detection |
| Query Engine | Trino on K8s | Iceberg + HDFS; Ranger-governed |
| BI | Apache Superset | Connected via Trino |
| ML | Spark / Jupyter on K8s | Governed curated Iceberg |
| Orchestration | Apache Airflow on K8s | KubernetesExecutor; DAGs for all pipelines |
| Monitoring | Prometheus + Grafana | Ranger, Vault, Kafka, pipeline metrics |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| PII detection | Spark classifier + OpenMetadata + Atlas | GDPR, HIPAA |
| Column masking | Ranger column masking policies | HIPAA, PCI |
| Row filtering | Ranger row-filter by jurisdiction | GDPR |
| Tokenization | Vault Transit FPE | HIPAA, PCI DSS |
| Dynamic secrets | Vault dynamic DB credentials | PCI DSS (no static credentials) |
| Encryption at rest | Vault KMS (on-prem HSM for vault zone) | PCI DSS, HIPAA |
| Encryption in transit | TLS + Vault mTLS (Vault PKI) | PCI DSS |
| Immutable audit | Kafka → WORM object store | SOX, HIPAA, PCI DSS |
| Lineage | Atlas + OpenMetadata | GDPR (erasure tracing) |
| Data residency | On-prem vault zone + cloud region isolation | GDPR |
| Right to erasure | Iceberg DELETE + Vault key deletion | GDPR |
| Certificate lifecycle | Vault PKI engine | PCI DSS (TLS cert rotation) |

---

## Comparison vs 9.9 (Hybrid Managed)

| Dimension | 9.10 Hybrid OSS | 9.9 Hybrid Managed |
|-----------|-----------------|-------------------|
| ETL / Ingestion | Debezium + Airbyte + NiFi | Informatica IDMC |
| Catalog | Atlas + OpenMetadata | Collibra SaaS |
| Access Control | Apache Ranger | Collibra policies |
| Tokenization | Vault Transit FPE | Protegrity |
| DB Monitoring | Ranger Audit + Vault | IBM Guardium |
| HSM Integration | Vault + Thales/SafeNet | Protegrity + Guardium native |
| Mainframe Support | Limited (custom adapters) | Native Informatica |
| Ops Overhead | Very High | Low-Medium |
| Cost Model | Infra + engineering | High SaaS license |
| Flexibility | Full control | Vendor roadmap |
