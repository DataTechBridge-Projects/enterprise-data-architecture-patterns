---
layout: default
title: "6.3 — Data Fabric · GCP Fully Managed"
---

# 6.3 — Data Fabric · GCP Fully Managed

**Stack:** Dataplex · Google Data Catalog · BigQuery Omni · Cloud DLP · Chronicle SIEM
**Processing:** Federated Query · No Data Movement · Active Metadata · Cross-Cloud Analytics
**Buy vs Build:** Buy (fully managed GCP-native fabric)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Physical Data Sources — Data Stays In Place"]
        S1[GCS Data Lake\nParquet · Iceberg · Avro]
        S2[(BigQuery\nanalytical warehouse)]
        S3[(Cloud SQL / Spanner\noperational)]
        S4[AWS S3\ncross-cloud via Omni]
        S5[Azure ADLS\ncross-cloud via Omni]
    end

    subgraph META["Active Metadata — Dataplex + Data Catalog"]
        M1[Dataplex Auto-discovery\nauto-scan GCS · BQ · SQL · Spanner]
        M2[Dataplex Data Catalog\ntechnical + business metadata · lineage]
        M3[Dataplex Zones\ncurated / raw zone management · data quality]
        M4[Cloud DLP\nPII auto-classification · de-identification]
    end

    subgraph GOV["Governance — IAM + VPC Service Controls"]
        G1[Google IAM + Conditions\nattribute-based fine-grained access]
        G2[BigQuery Column Security\npolicy tags · dynamic data masking]
        G3[VPC Service Controls\nperimeter enforcement]
    end

    subgraph QUERY["Federated Virtual Query — BigQuery"]
        Q1[BigQuery\nfederated query over GCS · Spanner · Cloud SQL]
        Q2[BigQuery Omni\ncross-cloud query AWS S3 · Azure ADLS in-place]
        Q3[BigQuery External Tables\nno data copy · schema-on-read]
    end

    subgraph CONSUME["Consumers"]
        F1[Looker / Looker Studio\nBI dashboards]
        F2[Vertex AI\nML training datasets]
        F3[Data Analysts\nBigQuery console · JDBC]
        F4[Dataplex Portal\nself-serve discovery]
    end

    SRC -. auto-discover .-> M1
    M1 --> M2 --> M3
    S1 & S2 -. scan .-> M4
    M2 -. tags .-> G1
    G1 --> G2
    G2 --> Q1
    Q1 -. federated .-> S1 & S2 & S3
    Q2 -. cross-cloud .-> S4 & S5
    Q3 --> F1 & F2 & F3
    M2 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Physical Data — In-Place"]
        A1[GCS Lake]
        A2[(BigQuery)]
        A3[(Cloud SQL)]
        A4[AWS S3]
        A5[Azure ADLS]
    end

    subgraph Metadata["Dataplex + Data Catalog"]
        B1[Dataplex Discovery\nauto schema]
        B2[Data Catalog\nasset registry]
        B3[Cloud DLP\nPII tags]
    end

    subgraph Policy["IAM + BQ Column Security"]
        C1[Policy tags\nPII · Confidential]
        C2[IAM Conditions\nattribute access]
    end

    subgraph Query["BigQuery Federated"]
        D1[BigQuery native\nGCS · Spanner · Cloud SQL]
        D2[BigQuery Omni\nAWS S3 · Azure ADLS]
    end

    subgraph Consume
        E1[Looker\ndashboards]
        E2[Vertex AI\nML]
        E3[Analysts\nSQL]
    end

    A1 & A2 & A3 -. discover .-> B1
    B1 --> B2
    A1 & A2 -. scan .-> B3
    B3 --> C1 --> C2
    C2 --> D1 & D2
    D1 -. query .-> A1 & A2 & A3
    D2 -. query .-> A4 & A5
    D1 & D2 --> E1 & E2 & E3
```

---

## Catalog Structure

```
Dataplex Lake: enterprise-data-fabric
├── Zone: raw (raw data zone)
│   ├── GCS bucket: gs://raw-data-lake/
│   │   ├── entity: clickstream (auto-discovered · Parquet)
│   │   └── entity: iot_events (auto-discovered · Avro)
│   └── BigQuery dataset: raw_external
│       └── external tables → GCS in-place
│
├── Zone: curated (curated data zone)
│   ├── GCS bucket: gs://curated-data-lake/
│   │   └── entity: customer_dim (Iceberg)
│   └── BigQuery dataset: curated
│       └── tables → BQ native or external Iceberg
│
└── Zone: trusted (high-quality governed zone)
    └── BigQuery dataset: gold
        └── tables → aggregated · BI-ready

Cross-Cloud (BigQuery Omni)
  AWS region: aws-us-east-1
    external table: s3://partner-data/transactions/ → query in-place
  Azure region: azure-eastus2
    external table: adls://collab-data/profiles/   → query in-place

Cloud DLP tags propagate from raw → curated → trusted zones.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Google IAM + BigQuery Column Security + VPC Service Controls    │
│                                                                  │
│  Google Identity / Group     Access Level     Scope              │
│  ──────────────────────────  ─────────────    ───────────────── │
│  data-engineers@             dataplex.editor  all lakes + zones  │
│  bi-analysts@                bigquery.dataViewer  curated + gold │
│  data-scientists@            bigquery.dataViewer  raw + curated  │
│  vertex-ai-sa@               storage.objectViewer  GCS zones     │
│  omni-query-sa@              bigquery.jobUser  cross-cloud query  │
│  audit-compliance@           dataplex.viewer  all + DLP findings │
│                                                                  │
│  BigQuery policy tags  → PII / PHI / PCI columns masked          │
│  IAM Conditions        → restrict by resource tag (zone)         │
│  VPC Service Controls  → GCS + BQ + Spanner in one perimeter     │
│  CMEK                  → Cloud KMS for GCS, BQ, Spanner          │
│  Dataplex data quality → reject assets failing quality rules     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Dataplex Discovery\nscheduled every 4h]
    T2[📥 GCS finalize event\nnew object in lake]
    T3[🔍 Cloud DLP Job\ndaily PII scan]

    T1 --> D1[Dataplex Discovery\ncrawl GCS + BQ + SQL]
    T2 --> D1
    D1 --> D2[Data Catalog update\nnew entities + schemas]
    D2 --> D3[Dataplex Data Quality\nrun quality rules on new entities]
    D3 --> D4{Quality pass?}
    D4 -->|yes| D5[Promote to curated zone\nautomated]
    D4 -->|no| D6[Alert via Cloud Monitoring\n→ Cloud Pub/Sub → data team]

    T3 --> P1[Cloud DLP Job\nscan GCS + BQ tables]
    P1 --> P2[Policy tag applied\nvia DLP auto-tag]
    P2 --> P3[BQ column masking enforced\nfor PII-tagged columns]

    D5 --> OQ[BigQuery Omni sync\nupdate cross-cloud external table defs]
    OQ --> CONS[Consumers query in-place\nno data movement]
```

---

## Component Map

| Component | GCP Service | Notes |
|-----------|------------|-------|
| Data Management | Dataplex | Unified data lake management: zones, quality, discovery, lineage |
| Technical Catalog | Google Data Catalog (within Dataplex) | Auto-discover GCS, BigQuery, Spanner, Cloud SQL assets |
| Data Quality | Dataplex Data Quality | Rule-based quality checks; auto-reject bad data from promotion |
| PII Detection | Cloud DLP API | 100+ detectors; auto-tag BigQuery columns + GCS objects |
| Federated Query | BigQuery native | External tables over GCS Parquet/Iceberg; Spanner federated joins |
| Cross-Cloud Query | BigQuery Omni | Query AWS S3 and Azure ADLS from BigQuery — no data copy |
| BI Access | Looker / Looker Studio | LookML semantic layer on BigQuery; real-time + SPICE |
| ML Access | Vertex AI Pipelines | Read BigQuery or GCS datasets; Vertex Feature Store |
| Access Control | Google IAM + BigQuery Column Security | Policy tags for column masking; IAM Conditions for attribute access |
| Network Control | VPC Service Controls | Service perimeter around GCS, BQ, Spanner, Dataplex |
| Encryption | Cloud KMS (CMEK) | All services BYOK; key rotation enforced |
| Security Monitoring | Chronicle SIEM | Ingest Cloud Audit Logs; threat detection + UEBA |
| Lineage | Dataplex Lineage API | Auto-capture from Dataflow, Spark, BigQuery jobs |

---

## Comparison vs 6.2 (Azure Data Fabric)

| Dimension | 6.3 GCP Managed | 6.2 Azure Managed |
|-----------|----------------|------------------|
| Metadata platform | Dataplex (integrated) | Microsoft Purview |
| Technical + business catalog | Unified in Dataplex | Unified in Purview |
| Cross-cloud query | BigQuery Omni (AWS + Azure) | Purview + Synapse (limited) |
| Zero-ETL operational | BQ federated Spanner | Synapse Link (Cosmos + SQL) |
| Data quality | Dataplex Data Quality built-in | Purview Data Quality (preview) |
| PII classification | Cloud DLP (100+ detectors) | Purview DLP |
| Access control | IAM Conditions + BQ policy tags | Purview Data Policy + RBAC |
| BI | Looker / Looker Studio | Power BI DirectLake |

---

## When to Choose This Implementation

✅ GCP is primary cloud; BigQuery is central analytical engine
✅ Cross-cloud query needed against AWS S3 or Azure ADLS (BigQuery Omni)
✅ Dataplex already manages data zones and quality rules
✅ Cloud DLP required for automated PII classification at scale
✅ Chronicle SIEM for unified security + data access monitoring

❌ Microsoft ecosystem deeply entrenched → use 6.2
❌ On-prem data residency critical → use 6.6 or 6.7
❌ Full OSS portability required → use 6.5
