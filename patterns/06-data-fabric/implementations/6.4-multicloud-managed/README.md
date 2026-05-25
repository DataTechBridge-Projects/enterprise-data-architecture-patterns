---
layout: default
title: "6.4 — Data Fabric · Multi-Cloud Fully Managed"
---

# 6.4 — Data Fabric · Multi-Cloud Fully Managed

**Stack:** Informatica IDMC · Collibra Data Intelligence Cloud · Starburst Galaxy · Talend Data Fabric
**Processing:** Federated Query · No Data Movement · Active Metadata · Cross-Cloud Governance
**Buy vs Build:** Buy (fully managed SaaS across AWS / Azure / GCP)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Physical Data Sources — Any Cloud or On-Prem"]
        S1[(AWS RDS / Redshift)]
        S2[AWS S3 / Iceberg]
        S3[(Azure SQL / Cosmos DB)]
        S4[Azure ADLS / Delta Lake]
        S5[GCS / BigQuery]
        S6[(On-prem Oracle / SAP)]
    end

    subgraph META["Active Metadata — Collibra + Informatica IDMC"]
        M1[Informatica IDMC\nauto-scan all sources · technical metadata]
        M2[Collibra Data Catalog\nbusiness glossary · stewardship · lineage]
        M3[Informatica CLAIRE AI\nAI-powered metadata recommendation + tagging]
        M4[Collibra DQ\ndata quality score per asset]
    end

    subgraph GOV["Governance — Collibra Policy Center"]
        G1[Collibra Policy Center\nbusiness policies → technical controls]
        G2[Informatica IDMC Access\nrow / column masking enforcement]
        G3[Talend Data Stewardship\nissue tracking · certification workflow]
    end

    subgraph QUERY["Federated Virtual Query — Starburst Galaxy"]
        Q1[Starburst Galaxy\nTrino-based SaaS federated engine]
        Q2[Galaxy Catalogs\nAWS · Azure · GCP · on-prem connectors]
        Q3[Galaxy Data Products\nversioned · governed · published]
    end

    subgraph CONSUME["Consumers"]
        F1[BI Tools\nTableau · Power BI · Looker via JDBC]
        F2[Data Scientists\nSpark · Python via Starburst JDBC]
        F3[App Teams\nREST / GraphQL via Starburst API]
        F4[Collibra Portal\nself-serve discovery + certification]
    end

    SRC -. scan .-> M1
    M1 --> M2
    M3 -. AI enrich .-> M2
    M2 -. quality .-> M4
    M2 -. policies .-> G1
    G1 --> G2
    G2 --> Q1
    Q1 --> Q2
    Q2 -. federated .-> S1 & S2 & S3 & S4 & S5 & S6
    Q1 --> Q3
    Q3 --> F1 & F2 & F3
    M2 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Physical Data — Multi-Cloud + On-Prem"]
        A1[(AWS RDS)]
        A2[S3 / Iceberg]
        A3[(Azure SQL)]
        A4[ADLS / Delta]
        A5[GCS / BQ]
        A6[(On-prem Oracle)]
    end

    subgraph Metadata["Collibra + Informatica IDMC"]
        B1[IDMC Scanner\nauto technical metadata]
        B2[Collibra Catalog\nbusiness glossary]
        B3[CLAIRE AI\nAI tagging]
        B4[Collibra DQ\nquality scores]
    end

    subgraph Policy["Collibra Policy Center"]
        C1[Business policies\n→ technical controls]
        C2[Masking rules\nrow filters]
    end

    subgraph Query["Starburst Galaxy"]
        D1[Federated engine\nTrino-based]
        D2[Catalog connectors\nall sources]
    end

    subgraph Consume
        E1[Tableau · Power BI\nBI]
        E2[Data Scientists\nJDBC]
        E3[Collibra Portal\nself-serve]
    end

    A1 & A2 & A3 & A4 & A5 & A6 -. scan .-> B1
    B1 --> B2
    B3 -. enrich .-> B2
    B2 -. score .-> B4
    B2 -. policies .-> C1 --> C2
    C2 --> D1
    D1 --> D2
    D2 -. federated read .-> A1 & A2 & A3 & A4 & A5 & A6
    D1 --> E1 & E2
    B2 --> E3
```

---

## Catalog Structure

```
Collibra Data Intelligence Cloud
├── Domain: Enterprise Data Catalog
│   ├── Business Glossary
│   │   ├── Term: Customer       → certified · owner: CRM
│   │   ├── Term: Transaction    → certified · owner: Finance
│   │   └── Term: Product        → draft · owner: Commerce
│   │
│   ├── Data Domain: Finance
│   │   ├── Data Set: Customer 360
│   │   │   ├── Asset: customers (AWS RDS)      → DQ score: 97%
│   │   │   └── Asset: transactions (S3 Iceberg) → DQ score: 99%
│   │   └── Data Set: GL Reporting
│   │       └── Asset: gl_entries (Oracle on-prem) → DQ score: 94%
│   │
│   └── Data Domain: Operations
│       └── Data Set: Inventory
│           ├── Asset: inventory (Azure SQL)     → DQ score: 91%
│           └── Asset: warehouse (ADLS Delta)    → DQ score: 95%

Informatica IDMC Technical Catalog
  Source: AWS RDS (prod-rds-cluster)    → 148 tables scanned
  Source: S3 (company-lake-prod)        → 2,400 objects
  Source: Azure SQL (prod-sql-01)       → 89 tables scanned
  Source: Oracle on-prem (erp-oracle)   → 312 tables scanned

All assets are virtual — Starburst Galaxy queries in-place.
No data is physically copied to the catalog layer.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Collibra Policy Center + Informatica IDMC + Starburst RBAC      │
│                                                                  │
│  Principal             Access Level     Scope                    │
│  ─────────────────── ─ ─────────────    ──────────────────────  │
│  data-steward          manage           Collibra certified assets │
│  data-engineer         SELECT + write   all Starburst catalogs   │
│  bi-analyst            SELECT (masked)  certified data products  │
│  data-scientist        SELECT           raw + curated domains    │
│  compliance-officer    SELECT + audit   all + DQ + policy view   │
│  app-service-sa        SELECT           approved data products   │
│                                                                  │
│  Collibra policies  → business rules → Informatica masking rules │
│  IDMC masking       → column-level dynamic masking at query time │
│  Starburst RBAC     → catalog-level + table + column grants      │
│  Row filters        → Starburst row-level security per role      │
│  Audit              → Collibra audit + Starburst query log       │
│  SSO                → Okta / Entra ID via SAML/OIDC              │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ IDMC Scheduled Scan\nevery 8h per source]
    T2[📋 Collibra Workflow\nnew asset certification request]
    T3[🔍 Collibra DQ Job\ndaily quality scan]

    T1 --> SC1[IDMC Scanner\ncrawl all sources]
    SC1 --> SC2[Collibra catalog update\nnew / changed assets]
    SC2 --> SC3[CLAIRE AI\nauto-suggest glossary links + owners]
    SC3 --> SC4[Data steward notified\nreview + accept suggestions]

    T2 --> WF1[Certification workflow\nsteward reviews · approves]
    WF1 --> WF2[Asset certified\npublished to Starburst Data Products]
    WF2 --> WF3[Starburst data product\nversioned · visible to consumers]

    T3 --> DQ1[Collibra DQ rules run\non registered assets]
    DQ1 --> DQ2{DQ score drop?}
    DQ2 -->|yes| DQ3[Alert steward\ndowngrade asset to Draft]
    DQ2 -->|no| DQ4[Score published\nto catalog]

    WF3 --> CONS[Consumer discovers\nvia Collibra portal]
    CONS --> Q1[Starburst Galaxy\nfederated query in-place]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Data Integration | Informatica IDMC | Cloud-native; 500+ connectors; AI-powered CLAIRE engine |
| Business Catalog | Collibra Data Intelligence Cloud | Business glossary · data products · stewardship workflows |
| Technical Catalog | Informatica IDMC Catalog | Auto-scan technical metadata from all sources |
| Data Quality | Collibra DQ (Owl Analytics) | Rule-based DQ scoring; anomaly detection |
| AI Metadata | Informatica CLAIRE | Auto-suggest lineage, owners, glossary links using ML |
| Data Stewardship | Talend Data Stewardship | Issue management · certification · correction workflows |
| Federated Query | Starburst Galaxy | Managed Trino SaaS; 50+ connectors; runs on customer VPC |
| Data Products | Starburst Galaxy Data Products | Versioned, governed, published views over federated data |
| BI Access | Tableau / Power BI / Looker | JDBC/ODBC to Starburst Galaxy |
| ML Access | Spark / Python | Starburst JDBC for bulk reads in training pipelines |
| Policy Enforcement | Collibra Policy Center | Business policies translated to IDMC masking + Starburst RBAC |
| Audit | Collibra Audit + Starburst logs | All access events; stewardship decisions tracked |
| Identity | Okta / Entra ID | SSO via SAML/OIDC; group-based role assignment |

---

## Comparison vs 6.5 (Multi-Cloud OSS)

| Dimension | 6.4 Multi-Cloud Managed | 6.5 Multi-Cloud OSS |
|-----------|------------------------|---------------------|
| Business catalog | Collibra (SaaS) | OpenMetadata (self-managed) |
| Technical catalog | Informatica IDMC (SaaS) | Apache Atlas (self-managed) |
| AI metadata | CLAIRE AI (built-in) | None out-of-box |
| Federated query | Starburst Galaxy (SaaS) | Trino (self-managed) |
| Data quality | Collibra DQ (integrated) | Custom rules via dbt |
| Policy enforcement | Collibra → IDMC (automated) | Ranger (manual config) |
| Ops overhead | Very low (all SaaS) | High (K8s + self-managed) |
| Vendor lock-in | High (Collibra + Informatica) | None |
| Cost at scale | High (per-user licensing) | Infrastructure only |

---

## When to Choose This Implementation

✅ Multi-cloud data spanning AWS + Azure + GCP + on-prem
✅ Formal data governance program with stewardship workflows required
✅ AI-powered metadata management (CLAIRE) to scale catalog curation
✅ Executive-level business catalog needed (Collibra data products)
✅ Minimal platform engineering team — all SaaS, no K8s

❌ Cost sensitivity → use 6.5 (OSS)
❌ Single cloud only → use 6.1 / 6.2 / 6.3 (cloud-native)
❌ On-prem only, no SaaS → use 6.7 (Hybrid OSS Self-Hosted)
