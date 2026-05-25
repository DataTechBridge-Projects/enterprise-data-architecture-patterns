---
layout: default
title: "6.5 — Data Fabric · Multi-Cloud OSS Portable"
---

# 6.5 — Data Fabric · Multi-Cloud OSS Portable

**Stack:** Apache Atlas · OpenMetadata · Apache Ranger · Trino · Apache Superset
**Processing:** Federated Query · No Data Movement · Active Metadata · Full OSS Portability
**Buy vs Build:** Build (fully portable OSS — runs on any cloud or on-prem K8s)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Physical Data Sources — Any Cloud or On-Prem"]
        S1[(PostgreSQL / MySQL\non any cloud)]
        S2[S3 / ADLS / GCS\nobject stores]
        S3[(Iceberg / Delta Lake\nopen table format)]
        S4[(MongoDB / Cassandra\nNoSQL)]
        S5[(On-prem Oracle\nSQL Server)]
    end

    subgraph META["Active Metadata — OpenMetadata + Apache Atlas"]
        M1[OpenMetadata Connectors\nauto-crawl schema · lineage · profiling]
        M2[OpenMetadata Catalog\nunified metadata UI · business glossary · owners]
        M3[Apache Atlas\nentity types · lineage graph · classification]
        M4[OpenMetadata Data Quality\nrule-based DQ profiling per asset]
    end

    subgraph GOV["Governance — Apache Ranger"]
        G1[Apache Ranger\ncolumn masking · row filters · tag-based policies]
        G2[Ranger Tag Sync\nAtlas tags → Ranger policies]
        G3[Ranger Audit\naccess log per resource + user]
    end

    subgraph QUERY["Federated Virtual Query — Trino"]
        Q1[Trino on Kubernetes\nfederated SQL engine]
        Q2[Trino Catalogs\nHive · Iceberg · Delta · JDBC · MongoDB connectors]
        Q3[Ranger Trino Plugin\npolicy enforcement at query time]
    end

    subgraph CONSUME["Consumers"]
        F1[Apache Superset\ndashboards via Trino JDBC]
        F2[Jupyter / Python\ndata science via Trino JDBC]
        F3[dbt Core\ntransformation via Trino]
        F4[OpenMetadata Portal\nself-serve discovery]
    end

    SRC -. crawl .-> M1
    M1 --> M2
    M1 -. entities + lineage .-> M3
    M2 -. quality .-> M4
    M3 -. tags .-> G2
    G2 --> G1
    G1 --> Q3
    Q3 --> Q1
    Q1 --> Q2
    Q2 -. federated .-> S1 & S2 & S3 & S4 & S5
    Q1 --> F1 & F2 & F3
    M2 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Physical Data — In-Place"]
        A1[(PostgreSQL)]
        A2[S3 / GCS / ADLS]
        A3[(Iceberg / Delta)]
        A4[(MongoDB)]
        A5[(Oracle on-prem)]
    end

    subgraph Metadata["OpenMetadata + Atlas"]
        B1[OM Connectors\nauto-crawl]
        B2[OpenMetadata\nUI + glossary]
        B3[Apache Atlas\nlineage graph]
        B4[OM Data Quality\nrules + profiling]
    end

    subgraph Policy["Apache Ranger"]
        C1[Tag sync\nAtlas → Ranger]
        C2[Column masking\nrow filters]
        C3[Audit log]
    end

    subgraph Query["Trino on K8s"]
        D1[Trino engine\nfederated SQL]
        D2[Ranger plugin\npolicy enforcement]
    end

    subgraph Consume
        E1[Superset\ndashboards]
        E2[Jupyter\ndata science]
        E3[OM Portal\nself-serve]
    end

    A1 & A2 & A3 & A4 & A5 -. crawl .-> B1
    B1 --> B2
    B1 -. entities .-> B3
    B2 -. quality .-> B4
    B3 -. tags .-> C1 --> C2 --> D2
    D2 --> D1
    D1 -. federated .-> A1 & A2 & A3 & A4 & A5
    D1 --> E1 & E2
    B2 --> E3
```

---

## Catalog Structure

```
OpenMetadata — Unified Metadata Platform
├── Services
│   ├── DatabaseService: postgres-prod       (PostgreSQL)
│   ├── DatabaseService: oracle-erp          (Oracle on-prem)
│   ├── StorageService: s3-data-lake         (AWS S3)
│   ├── StorageService: gcs-data-lake        (GCP GCS)
│   └── DashboardService: superset-prod      (Apache Superset)
│
├── Domains
│   ├── Finance
│   │   ├── Data Product: customer-360
│   │   │   ├── Table: postgres-prod.finance.customers  → DQ: 98%
│   │   │   └── Table: oracle-erp.fin.gl_entries        → DQ: 95%
│   │   └── Data Product: transactions
│   │       └── Topic: kafka.finance.transactions       → DQ: 99%
│   └── Operations
│       └── Data Product: inventory
│           └── Table: postgres-prod.ops.inventory      → DQ: 91%
│
└── Glossary
    ├── Term: Customer  → linked to 14 assets
    └── Term: Order     → linked to 9 assets

Apache Atlas — Lineage + Classification
  Entity types: hive_table · rdbms_table · kafka_topic · s3_object
  Classifications: PII · PHI · PCI · Confidential · Internal
  Lineage: source entity → process → target entity (auto-captured)

All assets are virtual — Trino queries in-place, no data copies.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Apache Ranger + OpenMetadata RBAC + Trino ACLs                  │
│                                                                  │
│  Principal             Access Level     Scope                    │
│  ─────────────────── ─ ─────────────    ──────────────────────  │
│  data-engineer         SELECT + DDL     all Trino catalogs       │
│  bi-analyst            SELECT (masked)  certified data products  │
│  data-scientist        SELECT           raw + curated catalogs   │
│  dbt-service-sa        SELECT + CREATE  curated + gold schemas   │
│  om-crawler-sa         SELECT (meta)    schema crawl only        │
│  compliance-sa         SELECT           Ranger audit + Atlas     │
│                                                                  │
│  Ranger tag policies  → PII tag → mask SSN, email, phone cols   │
│  Ranger row filter    → restrict by country / department attr    │
│  Trino system RBAC    → catalog · schema · table grants         │
│  OpenMetadata RBAC    → viewer / editor / owner per domain       │
│  Atlas REST auth      → Kerberos / basic / LDAP                  │
│  TLS                  → Trino coordinator + worker TLS           │
│  Secrets              → HashiCorp Vault for connector creds      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ OpenMetadata Ingestion\nscheduled every 6h]
    T2[⏰ Atlas Lineage Sync\nscheduled every 1h]
    T3[⏰ Ranger Tag Sync\nevery 30 min]

    T1 --> OM1[OM Connector runs\ncrawl schema + profiling]
    OM1 --> OM2[Metadata update in OM\nnew assets · schema changes]
    OM2 --> OM3[OM Data Quality checks\nrule execution on profiled tables]
    OM3 --> OM4{DQ alert?}
    OM4 -->|yes| OM5[Notify owner via OM\nslack webhook / email]
    OM4 -->|no| OM6[DQ score updated\nin OM catalog]

    T2 --> AT1[Atlas REST push\nnew entities + lineage edges]
    AT1 --> AT2[Atlas classification propagation\nupstream tags → downstream entities]

    T3 --> RG1[Ranger TagSync pulls Atlas tags\nPII · Confidential · PHI]
    RG1 --> RG2[Ranger policy evaluation\nmatch tag → apply masking rule]
    RG2 --> RG3[Trino Ranger plugin active\nnext query respects new policy]

    OM6 --> PORTAL[Data consumer discovers asset\nvia OpenMetadata portal]
    PORTAL --> QUERY[Trino federated query\nin-place on source system]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Unified Metadata | OpenMetadata | Modern OSS metadata platform; REST API; 50+ connectors; data quality |
| Lineage + Classification | Apache Atlas | Entity graph; auto-classification; tag propagation; Atlas REST API |
| Access Control | Apache Ranger | Tag-based + resource-based policies; column masking; row filters |
| Tag Sync | Ranger TagSync | Pulls classifications from Atlas → applies as Ranger tag policies |
| Federated Query | Trino on Kubernetes | Trino Kubernetes Operator; supports Hive, Iceberg, Delta, JDBC, MongoDB |
| Iceberg Catalog | Project Nessie / Hive Metastore | REST catalog for Iceberg; shared by Trino + Spark |
| BI Layer | Apache Superset | Trino JDBC connection; community dashboards |
| Transformation | dbt Core | Reads via Trino; pushes lineage to OpenMetadata |
| Data Profiling | OpenMetadata profiler | Column-level stats; completeness; uniqueness; distribution |
| Secrets Management | HashiCorp Vault | Dynamic secrets; connector credentials injected via K8s CSI |
| Identity | LDAP / Keycloak | SSO via OpenID Connect; Ranger LDAP group sync |
| Monitoring | Prometheus + Grafana | Trino cluster metrics; OM ingestion job status |
| Audit | Ranger Audit → Elasticsearch | Query-level audit trail; searchable via Kibana |

---

## Comparison vs 6.4 (Multi-Cloud Managed)

| Dimension | 6.5 Multi-Cloud OSS | 6.4 Multi-Cloud Managed |
|-----------|--------------------|-----------------------|
| Business catalog | OpenMetadata (OSS) | Collibra (SaaS) |
| Technical catalog | Apache Atlas (OSS) | Informatica IDMC (SaaS) |
| AI metadata | None built-in | CLAIRE AI (Informatica) |
| Federated query | Trino (self-managed) | Starburst Galaxy (SaaS) |
| Data quality | OpenMetadata profiler | Collibra DQ (Owl) |
| Policy enforcement | Ranger (manual config) | Collibra → IDMC (automated) |
| Ops overhead | High (K8s + 4 tools) | Very low (all SaaS) |
| Vendor lock-in | None | High |
| Cost at scale | Infrastructure only | High per-user licensing |

---

## When to Choose This Implementation

✅ Maximum portability — must run on any cloud or on-prem
✅ Zero vendor lock-in (no Collibra, Informatica, Starburst licensing)
✅ Strong platform engineering team familiar with K8s + OSS stack
✅ Cost optimization at large scale justifies ops investment
✅ Existing Apache Atlas or Ranger investment to build on

❌ Small platform team → use 6.4 (all SaaS, low ops)
❌ AI-powered metadata curation needed → use 6.4 (CLAIRE AI)
❌ On-prem only without K8s → use 6.7 (Hybrid OSS Self-Hosted)
