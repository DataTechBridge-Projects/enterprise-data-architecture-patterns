---
layout: default
title: "9.9 — Governed / Compliance-First · Hybrid Fully Managed"
---

# 9.9 — Governed / Compliance-First · Hybrid Fully Managed (On-Prem + Cloud)

**Stack:** Informatica IDMC · Collibra · IBM Guardium · Protegrity · Cloud KMS
**Processing:** Batch-first · Compliance-driven
**Buy vs Build:** Buy (enterprise SaaS governance on hybrid infra)
**Compliance Targets:** GDPR · HIPAA · PCI DSS · SOX · Basel III

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources — Hybrid"]
        S1[On-Prem RDBMS\nOracle · SQL Server · DB2]
        S2[Mainframe / Legacy\nIBM z/OS · VSAM]
        S3[Cloud DW\nRedshift · Synapse · BigQuery]
        S4[SaaS / Files]
    end

    subgraph INGEST["Ingestion — Informatica IDMC"]
        I1[Informatica IICS\nPowerCenter-compatible ETL]
        I2[Informatica CDC\nDebezium-backed CDC]
        I3[Informatica Mass Ingestion\nbulk + incremental]
    end

    subgraph CLASSIFY["Classification — Informatica + Collibra"]
        CL1[Informatica Axon\nauto PII / PHI discovery]
        CL2[Collibra DQ\ndata quality + tagging]
        CL3[Collibra Business Glossary\nsensitivity classification]
    end

    subgraph STORAGE["Storage — Hybrid"]
        Z1[ON-PREM LANDING\nNetApp / Pure Storage\nencrypted at rest]
        Z2[ON-PREM RAW\nHadoop HDFS / NetApp\nProtegrity-encrypted]
        Z3[CLOUD CURATED\nS3 / ADLS / GCS\nKMS-encrypted masked]
        Z4[VAULT\nOn-prem HSM vault\nPII isolated]
    end

    subgraph GOVERN["Governance — Collibra + Guardium"]
        G1[Collibra Data Governance\npolicies + stewardship]
        G2[IBM Guardium\nDB activity monitoring]
        G3[Protegrity\ntokenization + masking]
        G4[Collibra Lineage\nend-to-end lineage]
    end

    subgraph AUDIT["Audit"]
        A1[IBM Guardium Audit\nall DB activity]
        A2[Collibra Audit\ncatalog + policy changes]
        A3[SIEM — Splunk / QRadar\ncentralized security events]
    end

    subgraph CONSUME["Consumption"]
        F1[Enterprise BI\nCognos · SAP BO · Tableau]
        F2[Cloud Analytics\nRedshift · BigQuery]
        F3[Data Science\nDataiku · SAS]
        F4[Regulatory Reports\nautomated compliance exports]
    end

    SRC --> INGEST
    INGEST --> Z1
    Z1 --> CL1 & CL2 --> CL3
    CL3 -->|PII| Z4
    CL3 -->|clean| Z2
    Z2 -->|ETL to cloud| Z3

    Z2 & Z3 -. register .-> G4
    G1 -. policies .-> G3
    G3 -. mask .-> F1 & F2 & F3
    G2 -. monitor .-> Z1 & Z2

    G2 --> A1
    G4 --> A2
    A1 & A2 --> A3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["On-Prem + Cloud"]
        A1[(Oracle DB)]
        A2[Mainframe]
        A3[Cloud SaaS]
    end

    subgraph Ingestion["Informatica IDMC"]
        B1[IICS ETL]
        B2[CDC]
        B3[Mass Ingestion]
    end

    subgraph Classification["Axon + Collibra DQ"]
        C1[PII Detection]
        C2[Sensitivity Tags]
    end

    subgraph Storage["On-Prem + Cloud Zones"]
        D1[🔒 On-Prem Landing]
        D2[🔒 On-Prem Raw\nProtegrity]
        D3[🔒 Cloud Curated\nMasked]
        D4[🔐 On-Prem Vault\nHSM]
    end

    subgraph Governance["Collibra + Guardium\n+ Protegrity"]
        E1[Masking\nProtegrity]
        E2[DB Monitoring\nGuardium]
        E3[Policy\nCollibra]
    end

    subgraph Consume
        F1[Enterprise BI]
        F2[Cloud Analytics]
        F3[Regulatory Reports]
    end

    A1 --> B1 --> D1
    A2 --> B2 --> D1
    A3 --> B3 --> D1

    D1 --> C1 --> C2
    C2 -->|PII| D4
    C2 -->|clean| D2 --> D3

    D2 & D3 --> E1 & E2 & E3
    E1 & E2 & E3 --> F1 & F2 & F3
```

---

## Zone Design

```
Hybrid Zone Layout:
│
├── ON-PREM
│   ├── landing/     → NAS / SAN · Protegrity encryption · 7-day retention
│   ├── raw/         → HDFS / NetApp · Protegrity column encryption
│   └── vault/       → Dedicated HSM appliance · PII fields only
│                       IBM Guardium monitors all access
│
└── CLOUD  (S3 / ADLS / GCS)
    ├── curated/     → KMS-encrypted · PII tokenized by Protegrity
    │   └── {domain}/{entity}/year=YYYY/month=MM/
    └── archive/     → Glacier / Archive tier · compliance retention
        └── 7-year retention for SOX / Basel III
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Collibra Policy + IBM Guardium + Protegrity                     │
│                                                                  │
│  Role                  Access Level   Zone          Protection   │
│  ──────────────────    ────────────   ──────────    ─────────── │
│  compliance-admin      Read+Write     All           Full         │
│  data-engineer         Read+Write     Raw+Curated   Protegrity   │
│  data-analyst          Read only      Curated       Tokenized    │
│  bi-consumer           Read only      Curated       Tokenized    │
│  auditor               Read only      Audit only    No PII       │
│  vault-access          Read only      Vault         Authorized   │
│  regulator-report      Read only      Curated       Masked       │
│                                                                  │
│  Protegrity     → format-preserving tokenization for PII/PAN   │
│  Guardium       → real-time DB activity monitoring + blocking   │
│  Collibra       → policy enforcement + stewardship workflows    │
│  On-prem HSM    → Thales/SafeNet key management appliance      │
│  Cloud KMS      → CMK for cloud curated zone                   │
│  Network        → MPLS / VPN for on-prem ↔ cloud transit      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Informatica Schedule\ndaily 01:00 local]

    T1 --> J1[Informatica IICS\nLanding → Raw\nclassify + tag]
    J1 --> J2[Protegrity Job\nPII tokenization\nRaw → Vault or encrypt-in-place]
    J2 --> J3[Informatica IICS\nRaw → Cloud Curated\nmask + conform]
    J3 --> J4[Collibra\nupdate lineage + policy sync]
    J4 --> J5[BI Refresh\ncognos / Tableau reload]
    J5 --> N1[Email Alert\npipeline complete]

    J1 -->|fail| ERR[Guardium Alert\n→ Splunk → PagerDuty]
    J3 -->|fail| ERR

    G1[IBM Guardium\nreal-time DB monitoring] -->|block anomaly| BLK[Block + Alert\nSecurity Team]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| ETL / Ingestion | Informatica IDMC (IICS) | PowerCenter-compatible; 400+ connectors |
| CDC | Informatica CDC | Log-based CDC for Oracle, SQL Server, DB2 |
| Bulk Ingestion | Informatica Mass Ingestion | Mainframe + file sources |
| PII Discovery | Informatica Axon Data Marketplace | Auto-classifies sensitive data |
| Data Quality | Collibra DQ | Rule-based quality + tagging |
| Schema Catalog | Collibra Data Catalog | End-to-end lineage; stewardship |
| Business Glossary | Collibra | Sensitivity classification + policy terms |
| Access Policies | Collibra Data Governance | Policy definition + stewardship workflow |
| DB Activity Monitor | IBM Guardium | Real-time query monitoring + blocking |
| Tokenization | Protegrity Data Security Platform | FPE tokenization; column-level encryption |
| On-Prem Encryption | Protegrity + on-prem HSM (Thales) | Key management for on-prem zones |
| Cloud Encryption | AWS KMS / Azure Key Vault / Cloud KMS | CMK for cloud curated zone |
| On-Prem Storage | NetApp / HDFS | Protegrity-encrypted landing + raw |
| Cloud Storage | S3 / ADLS / GCS | KMS-encrypted curated + archive |
| BI | Cognos / SAP BusinessObjects / Tableau | Governed via Collibra policies |
| Cloud Analytics | Redshift / BigQuery / Synapse | Governed curated data |
| SIEM | Splunk / IBM QRadar | Guardium + Collibra events |
| Network | MPLS / VPN / ExpressRoute/DirectConnect | Encrypted on-prem ↔ cloud transit |

---

## Compliance Controls Matrix

| Control | Mechanism | Regulation |
|---------|-----------|------------|
| PII discovery | Informatica Axon + Collibra DQ | GDPR, HIPAA |
| Tokenization | Protegrity FPE | HIPAA, PCI DSS |
| Column encryption | Protegrity column-level | PCI DSS |
| DB activity monitoring | IBM Guardium real-time | PCI DSS, SOX, HIPAA |
| Encryption at rest | Protegrity (on-prem) + KMS (cloud) | PCI DSS, HIPAA |
| Encryption in transit | TLS + MPLS / VPN | PCI DSS |
| Audit trail | Guardium + Collibra → SIEM | SOX, HIPAA, Basel III |
| Long-term retention | Cloud archive (7+ years) | SOX, Basel III |
| Stewardship workflow | Collibra approval workflows | Internal + regulatory |
| Right to erasure | Protegrity key deletion + data purge | GDPR |

---

## Comparison vs 9.10 (Hybrid OSS)

| Dimension | 9.9 Hybrid Managed | 9.10 Hybrid OSS |
|-----------|-------------------|-----------------|
| ETL | Informatica IDMC | Spark + Airbyte |
| Catalog | Collibra SaaS | Apache Atlas + OpenMetadata |
| Tokenization | Protegrity | HashiCorp Vault Transit |
| DB Monitoring | IBM Guardium | Apache Ranger + custom audit |
| Mainframe Support | Native (Informatica) | Limited (custom adapters) |
| Ops Overhead | Low-Medium (managed SaaS) | Very High (self-hosted) |
| Cost Model | High license cost | Infra + engineering cost |
| Legacy / Mainframe | Best fit | Difficult |
