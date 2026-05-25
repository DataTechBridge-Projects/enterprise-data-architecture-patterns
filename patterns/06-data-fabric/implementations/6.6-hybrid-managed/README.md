---
layout: default
title: "6.6 — Data Fabric · Hybrid Fully Managed"
---

# 6.6 — Data Fabric · Hybrid Fully Managed (On-Prem + Cloud)

**Stack:** Informatica IDMC · Collibra Data Intelligence Cloud · IBM Cloud Pak for Data · Watson Knowledge Catalog
**Processing:** Federated Query · No Data Movement · Active Metadata · On-Prem + Cloud Unified Governance
**Buy vs Build:** Buy (managed SaaS control plane + on-prem compute agents)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph ONPREM["On-Prem Physical Data — Data Stays In Place"]
        S1[(Oracle DB\nERP · Finance)]
        S2[(IBM Db2\noperational)]
        S3[(SQL Server\noperational)]
        S4[NAS / HDFS\nfiles · Parquet]
        S5[SAP ERP\nS/4HANA]
    end

    subgraph CLOUD["Cloud Physical Data — Data Stays In Place"]
        S6[AWS S3 / Redshift]
        S7[Azure ADLS / Synapse]
        S8[GCP GCS / BigQuery]
    end

    subgraph AGENT["On-Prem Secure Agents — No Firewall Holes"]
        A1[Informatica IDMC Secure Agent\non-prem crawl + integration]
        A2[IBM Cloud Pak for Data\noperator on OpenShift / K8s]
        A3[Watson Knowledge Catalog Agent\nmetadata harvest]
    end

    subgraph META["Active Metadata — Collibra + Informatica IDMC"]
        M1[IDMC Cloud Catalog\ntechnical metadata · schema · lineage]
        M2[Collibra Data Intelligence Cloud\nbusiness glossary · policies · stewardship]
        M3[Watson Knowledge Catalog\non-prem asset registry · IBM data products]
        M4[CLAIRE AI\nAI-powered metadata suggestions]
    end

    subgraph GOV["Governance & Policy"]
        G1[Collibra Policy Center\nbusiness policies → controls]
        G2[IBM Data Privacy Passports\non-prem data privacy enforcement]
        G3[Informatica IDMC Masking\ncolumn-level dynamic masking]
    end

    subgraph QUERY["Federated Virtual Query"]
        Q1[IBM Watson Query\non-prem federated SQL over all sources]
        Q2[Informatica IDMC CDI\nvirtual data integration layer]
    end

    subgraph CONSUME["Consumers"]
        F1[IBM Cognos / Tableau\nBI dashboards]
        F2[IBM Watson Studio\nML notebooks]
        F3[Collibra Portal\nself-serve discovery]
        F4[Business Users\nCertified data products]
    end

    ONPREM --> A1 & A2 & A3
    A1 -. crawl .-> M1
    A2 & A3 -. crawl .-> M3
    CLOUD -. crawl via IDMC .-> M1
    M1 --> M2
    M4 -. AI enrich .-> M2
    M3 --> M2
    M2 -. policies .-> G1
    G1 --> G2 & G3
    G3 --> Q1 & Q2
    Q1 -. federated .-> ONPREM & CLOUD
    Q2 -. virtual .-> ONPREM & CLOUD
    Q1 & Q2 --> F1 & F2
    M2 --> F3 & F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph OnPrem["On-Prem Sources"]
        A1[(Oracle)]
        A2[(Db2)]
        A3[(SQL Server)]
        A4[HDFS / NAS]
    end

    subgraph CloudSrc["Cloud Sources"]
        B1[S3 / ADLS / GCS]
        B2[(Redshift / Synapse / BQ)]
    end

    subgraph Agents["Secure Agents (on-prem)"]
        C1[IDMC Secure Agent]
        C2[IBM CPD Agent]
    end

    subgraph Metadata["Collibra + IDMC + WKC"]
        D1[IDMC Catalog\ntechnical metadata]
        D2[Collibra\nbusiness catalog]
        D3[Watson KCatalog\nIBM data products]
    end

    subgraph Policy["Governance"]
        E1[Collibra policies]
        E2[IBM Privacy Passports]
        E3[IDMC masking]
    end

    subgraph Query["Federated Query"]
        F1[IBM Watson Query\nfederated SQL]
        F2[IDMC CDI\nvirtual layer]
    end

    subgraph Consume
        G1[Cognos · Tableau]
        G2[Watson Studio]
        G3[Collibra Portal]
    end

    A1 & A2 & A3 & A4 --> C1 & C2
    B1 & B2 --> D1
    C1 -. crawl .-> D1
    C2 -. crawl .-> D3
    D1 --> D2
    D3 --> D2
    D2 -. enforce .-> E1 --> E2 & E3
    E3 --> F1 & F2
    F1 -. federate .-> A1 & A2 & A3 & A4 & B1 & B2
    F1 & F2 --> G1 & G2
    D2 --> G3
```

---

## Catalog Structure

```
Collibra Data Intelligence Cloud (SaaS control plane)
├── Domain: Finance (on-prem primary)
│   ├── Data Product: General Ledger
│   │   ├── Asset: Oracle.fin.gl_accounts     → certified · DQ: 96%
│   │   └── Asset: Db2.fin.journal_entries    → certified · DQ: 94%
│   └── Data Product: Customer Master
│       └── Asset: SQL Server.crm.customers   → certified · DQ: 98%
│
├── Domain: Supply Chain (hybrid)
│   ├── Data Product: Inventory
│   │   ├── Asset: SAP.MM.inventory           → draft · DQ: 88%
│   │   └── Asset: S3.supply.warehouse_snap   → certified · DQ: 95%
│   └── Data Product: Orders
│       └── Asset: Oracle.scm.order_lines     → certified · DQ: 97%
│
└── Domain: Analytics (cloud)
    └── Data Product: Executive Reporting
        ├── Asset: Redshift.gold.financials    → certified
        └── Asset: BigQuery.gold.sales         → certified

IBM Watson Knowledge Catalog (on-prem)
  Namespace: enterprise-data
    Catalog: Finance Assets          → 312 assets · governed
    Catalog: Operations Assets       → 189 assets · governed

All assets virtual — Watson Query + IDMC CDI resolve at query time.
Secure Agents relay metadata and queries without opening inbound ports.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Collibra Policy + IBM Data Privacy Passports + IDMC Masking     │
│                                                                  │
│  Principal             Access Level     Scope                    │
│  ─────────────────── ─ ─────────────    ──────────────────────  │
│  data-steward          manage           all Collibra domains     │
│  data-engineer         SELECT + write   on-prem + cloud sources  │
│  bi-analyst            SELECT (masked)  certified data products  │
│  data-scientist        SELECT           approved domains         │
│  compliance-officer    SELECT + audit   all + Privacy Passport   │
│  ibm-cpd-sa            SELECT           Watson Query + Studio    │
│                                                                  │
│  IDMC masking      → column-level dynamic masking on all sources │
│  Privacy Passports → on-prem policy enforcement (no cloud exit)  │
│  Collibra policies → steward-approved access before publish      │
│  Secure Agent      → TLS tunnel outbound only (no inbound ports) │
│  IBM CPD auth      → LDAP / Kerberos / SAML for on-prem users   │
│  Network           → MPLS / VPN between on-prem and cloud fabric │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ IDMC Secure Agent\nscheduled crawl every 8h]
    T2[⏰ IBM CPD Agent\ncrawl on-prem IBM assets every 8h]
    T3[📋 Collibra Certification\nworkflow triggered on new asset]

    T1 --> SC1[IDMC crawl Oracle · Db2 · SQL Server]
    SC1 --> SC2[IDMC Catalog update\n→ push to Collibra]
    T2 --> SC3[Watson KCatalog update\nnew IBM data assets]
    SC3 --> SC2

    SC2 --> WF1[Collibra workflow triggered\nsteward assigned]
    WF1 --> WF2[Steward reviews asset\nlinks glossary · sets DQ threshold]
    WF2 --> WF3[Asset certified\npublished to consumers]

    T3 --> CER1[Owner approves subscription\naccess granted via IDMC masking]
    CER1 --> CER2[IBM Watson Query\nresolves federated query on approval]

    CER2 --> CONS[Consumer queries in-place\nno data leaves source system]
    CONS -->|audit event| AUD[Collibra Audit + IBM Activity Tracker\ncompliance record]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Integration Engine | Informatica IDMC (Cloud Data Integration) | Secure Agents for on-prem; 500+ connectors; no inbound firewall |
| Business Catalog | Collibra Data Intelligence Cloud | Business glossary · stewardship · certification · policy center |
| On-Prem Catalog | IBM Watson Knowledge Catalog | IBM data products; integrates with Db2, Cognos, Watson Studio |
| AI Metadata | Informatica CLAIRE | Auto-suggest lineage, owners, glossary terms via ML |
| On-Prem Compute | IBM Cloud Pak for Data (OpenShift) | Watson Studio · Watson Query · WKC on on-prem K8s |
| Federated Query | IBM Watson Query | Federated SQL over IBM Db2, Oracle, S3, Netezza, Kafka |
| Virtual Integration | Informatica IDMC CDI | Virtual data integration; no physical movement |
| Privacy Enforcement | IBM Data Privacy Passports | On-prem policy enforcement; data remains sovereign |
| Column Masking | Informatica IDMC dynamic masking | Column-level masking based on Collibra sensitivity policies |
| BI Access | IBM Cognos Analytics / Tableau | Cognos native on CPD; Tableau via JDBC to Watson Query |
| ML Access | IBM Watson Studio | Notebooks + AutoML; reads via Watson Query |
| Identity | LDAP / Kerberos (on-prem) · Entra ID (cloud) | Federated SSO via SAML |
| Network | IPSec VPN / MPLS | Secure Agent outbound only; no inbound ports on on-prem |
| Audit | IBM Activity Tracker + Collibra Audit | All data access events; compliance reporting |

---

## Comparison vs 6.7 (Hybrid OSS Self-Hosted)

| Dimension | 6.6 Hybrid Managed | 6.7 Hybrid OSS Self-Hosted |
|-----------|-------------------|---------------------------|
| Business catalog | Collibra (SaaS) | DataHub (self-managed) |
| Technical catalog | Informatica IDMC (SaaS) | Apache Atlas (self-managed) |
| On-prem catalog | IBM Watson KCatalog | Apache Atlas + Ranger |
| Federated query | IBM Watson Query | Trino on K8s |
| Privacy enforcement | IBM Privacy Passports | Apache Ranger |
| On-prem compute | IBM Cloud Pak for Data | K8s + all OSS |
| Ops overhead | Low (agents only on-prem) | High (full K8s stack) |
| IBM ecosystem | Deep integration | Not applicable |
| Vendor lock-in | High (Collibra + IBM) | None |

---

## When to Choose This Implementation

✅ Existing IBM infrastructure (Db2, Cognos, Watson Studio, OpenShift)
✅ Strict data residency — data never leaves on-prem; only metadata to cloud SaaS
✅ Formal data governance program with Collibra already licensed
✅ Secure Agent model required — no inbound firewall changes
✅ Hybrid workforce: on-prem analysts + cloud data scientists

❌ No IBM investment → use 6.4 (Informatica + Collibra + Starburst)
❌ Full OSS preference → use 6.7
❌ Cloud-only data → use 6.4 or 6.1/6.2/6.3
