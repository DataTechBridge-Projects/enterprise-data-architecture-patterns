---
layout: default
title: "6.7 — Data Fabric · Hybrid OSS Self-Hosted"
---

# 6.7 — Data Fabric · Hybrid OSS Self-Hosted (On-Prem + Cloud)

**Stack:** Apache Atlas · DataHub · Apache Ranger · Trino on Kubernetes · Apache Superset
**Processing:** Federated Query · No Data Movement · Active Metadata · Full Build
**Buy vs Build:** Full Build (self-hosted OSS — maximum control, highest ops cost)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph ONPREM["On-Prem Physical Data — Data Stays In Place"]
        S1[(Oracle DB\nERP · Finance)]
        S2[(PostgreSQL\noperational)]
        S3[(SQL Server\nHR · CRM)]
        S4[MinIO / HDFS\nfiles · Parquet · Iceberg]
        S5[Kafka on K8s\nevent streams]
    end

    subgraph CLOUD["Cloud Physical Data — Data Stays In Place"]
        S6[AWS S3 / Iceberg]
        S7[Azure ADLS / Delta]
        S8[GCP GCS]
    end

    subgraph META["Active Metadata — DataHub + Apache Atlas"]
        M1[DataHub Ingestion Sources\nauto-crawl schema · lineage · ownership]
        M2[DataHub Metadata Platform\nunified search · business glossary · data products]
        M3[Apache Atlas\nentity types · lineage graph · classification tags]
        M4[DataHub Data Quality\nassertion-based quality checks]
    end

    subgraph GOV["Governance — Apache Ranger on K8s"]
        G1[Ranger Admin\npolicy management UI]
        G2[Ranger TagSync\nAtlas tags → Ranger policies]
        G3[Ranger Plugins\nTrino · Hive · HDFS · Kafka enforcement]
        G4[Ranger Audit → Elasticsearch\naccess log + compliance]
    end

    subgraph QUERY["Federated Virtual Query — Trino on Kubernetes"]
        Q1[Trino Coordinator\nK8s Deployment]
        Q2[Trino Workers\nK8s auto-scale]
        Q3[Catalogs\nJDBC · Hive · Iceberg · Delta · MongoDB · Kafka]
        Q4[Ranger Trino Plugin\npolicy enforcement at query time]
    end

    subgraph CONSUME["Consumers"]
        F1[Apache Superset\ndashboards via Trino JDBC]
        F2[Jupyter / Python\ndata science via Trino JDBC]
        F3[dbt Core\ntransformation via Trino]
        F4[DataHub Portal\nself-serve discovery + subscriptions]
    end

    ONPREM & CLOUD -. crawl .-> M1
    M1 --> M2
    M1 -. entities .-> M3
    M2 -. quality .-> M4
    M3 -. tags .-> G2
    G2 --> G1 --> G3
    G3 --> Q4 --> Q1
    Q1 --> Q2
    Q2 --> Q3
    Q3 -. federated .-> ONPREM & CLOUD
    Q1 --> F1 & F2 & F3
    M2 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph OnPrem["On-Prem Sources"]
        A1[(Oracle)]
        A2[(PostgreSQL)]
        A3[(SQL Server)]
        A4[MinIO / HDFS]
        A5[Kafka]
    end

    subgraph Cloud["Cloud Sources"]
        B1[S3 / Iceberg]
        B2[ADLS / Delta]
        B3[GCS]
    end

    subgraph Metadata["DataHub + Atlas"]
        C1[DataHub Ingestion\nauto-crawl]
        C2[DataHub\nUI · glossary · products]
        C3[Atlas\nlineage + tags]
        C4[DataHub DQ\nassertions]
    end

    subgraph Policy["Apache Ranger"]
        D1[TagSync\nAtlas → Ranger]
        D2[Ranger policies\nmasking · row filters]
        D3[Audit → ES\naccess log]
    end

    subgraph Query["Trino on K8s"]
        E1[Trino\nfederated SQL]
        E2[Ranger plugin\nenforcement]
    end

    subgraph Consume
        F1[Superset\ndashboards]
        F2[Jupyter\ndata science]
        F3[DataHub Portal\nself-serve]
    end

    A1 & A2 & A3 & A4 & A5 & B1 & B2 & B3 -. crawl .-> C1
    C1 --> C2
    C1 -. entities .-> C3
    C2 -. quality .-> C4
    C3 -. tags .-> D1 --> D2 --> E2
    E2 --> E1
    E1 -. federate .-> A1 & A2 & A3 & A4 & B1 & B2 & B3
    E1 --> F1 & F2
    C2 --> F3
    E1 --> D3
```

---

## Catalog Structure

```
DataHub — Unified Metadata Platform (on-prem K8s)
├── Platforms
│   ├── oracle        → 318 datasets · 12 owners assigned
│   ├── postgresql    → 145 datasets · 8 owners assigned
│   ├── mssql         → 89 datasets  · 5 owners assigned
│   ├── minio         → 1,240 S3 paths · partitioned assets
│   ├── iceberg       → 380 tables (on S3 + ADLS)
│   └── kafka         → 64 topics · schema registry linked
│
├── Domains
│   ├── Finance
│   │   ├── Data Product: gl-reporting       → certified
│   │   └── Data Product: customer-master    → certified
│   ├── Operations
│   │   ├── Data Product: inventory-snapshot → draft
│   │   └── Data Product: order-pipeline     → certified
│   └── Analytics
│       └── Data Product: executive-kpis     → certified
│
└── Glossary
    ├── Term: Customer  → 22 assets linked
    ├── Term: Order     → 15 assets linked
    └── Term: Product   → 11 assets linked

Apache Atlas (on-prem K8s — lineage + classification)
  Entity types: rdbms_table · hive_table · kafka_topic · hdfs_path
  Classifications: PII · PHI · PCI · Confidential · Internal
  Lineage graph: Spark jobs + dbt runs auto-push entities

All assets are virtual — Trino queries in-place. No data moved.
Secure Agents pattern: Trino workers co-located on on-prem K8s.
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Apache Ranger + DataHub RBAC + Trino ACLs + HashiCorp Vault     │
│                                                                  │
│  Principal             Access Level     Scope                    │
│  ─────────────────── ─ ─────────────    ──────────────────────  │
│  data-engineer         SELECT + DDL     all Trino catalogs       │
│  bi-analyst            SELECT (masked)  certified data products  │
│  data-scientist        SELECT           raw + curated catalogs   │
│  dbt-service-sa        SELECT + CREATE  curated + gold schemas   │
│  datahub-crawler-sa    SELECT (meta)    schema crawl only        │
│  atlas-admin           admin            entity types · tags      │
│  compliance-sa         SELECT + audit   Ranger audit + Elastic   │
│                                                                  │
│  Ranger tag policies → PII tag → mask SSN · email · phone       │
│  Ranger row filter   → restrict by department · country          │
│  Trino system ACLs   → catalog · schema · table grants          │
│  DataHub RBAC        → viewer / editor / owner per domain        │
│  HashiCorp Vault     → dynamic secrets for Trino JDBC connectors │
│  Network policies    → K8s NetworkPolicy: Trino → source only   │
│  mTLS                → Trino coordinator ↔ worker TLS           │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ DataHub Ingestion\nscheduled every 4h]
    T2[⏰ Atlas Lineage Sync\nevery 1h from Spark jobs]
    T3[⏰ Ranger TagSync\nevery 30 min]
    T4[📋 DataHub Data Product\ncertification workflow]

    T1 --> DH1[DataHub recipe runs\ncrawl Oracle · PG · MinIO · Iceberg · Kafka]
    DH1 --> DH2[Metadata update in DataHub\nnew datasets · schema changes · owners]
    DH2 --> DH3[DataHub assertions run\ndata quality checks]
    DH3 --> DH4{Assertion fail?}
    DH4 -->|yes| DH5[Notify dataset owner\nslack webhook via DataHub]
    DH4 -->|no| DH6[DQ score updated\nin DataHub catalog]

    T2 --> AT1[Spark / dbt push lineage\nAtlas REST API]
    AT1 --> AT2[Atlas entity created/updated\nlineage edge added]
    AT2 --> AT3[PII tag propagates\nupstream → downstream assets]

    T3 --> RG1[Ranger TagSync polls Atlas\nPII · Confidential tags]
    RG1 --> RG2[Ranger policy updated\nmask rule applied to tagged columns]

    T4 --> WF1[Domain owner reviews\nlinks glossary · sets SLA]
    WF1 --> WF2[Data product certified\npublished in DataHub]
    WF2 --> CONS[Consumer discovers\nvia DataHub portal]
    CONS --> Q1[Trino federated query\nin-place — no data copy]
    Q1 -->|all queries| AUD[Ranger Audit\n→ Elasticsearch → Kibana]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Unified Metadata | DataHub (LinkedIn OSS) | Modern metadata platform; REST API; 40+ ingestion sources; data products |
| Lineage + Classification | Apache Atlas | Entity graph; auto-classification; tag propagation; Ranger TagSync |
| Access Control | Apache Ranger on K8s | Tag-based + resource policies; column masking; row filters; audit |
| Ranger TagSync | Ranger TagSync daemon | Pulls Atlas classifications → creates Ranger tag policies automatically |
| Federated Query | Trino on Kubernetes (Trino Operator) | Coordinators + workers on K8s; auto-scale; 30+ catalogs |
| Iceberg Catalog | Project Nessie / Hive Metastore | REST catalog shared by Trino + Spark; git-like branching (Nessie) |
| Delta Lake Catalog | Delta Sharing / Hive Metastore | Delta table metadata for ADLS sources |
| On-Prem Storage | MinIO (S3-compatible) | Erasure coding; S3A compatible; Trino + Spark native |
| BI Layer | Apache Superset | Trino JDBC; community dashboards; LDAP auth |
| Transformation | dbt Core | Reads via Trino; emits lineage to DataHub via dbt-datahub plugin |
| Data Profiling | DataHub GMS profiler | Column stats; completeness; pushed to DataHub metadata |
| Secrets | HashiCorp Vault + K8s CSI | Dynamic secrets for Oracle/PG/SQL Server JDBC credentials |
| Identity | LDAP / Keycloak | OIDC for DataHub; Kerberos for Hadoop/Atlas; Ranger LDAP groups |
| Monitoring | Prometheus + Grafana on K8s | Trino JVM metrics; DataHub GMS health; Ranger plugin latency |
| Audit Storage | Ranger Audit → Elasticsearch | All query-level events; Kibana dashboard for compliance |

---

## Comparison vs 6.6 (Hybrid Managed)

| Dimension | 6.7 Hybrid OSS Self-Hosted | 6.6 Hybrid Managed |
|-----------|---------------------------|-------------------|
| Business catalog | DataHub (OSS) | Collibra (SaaS) |
| Technical catalog | Apache Atlas (OSS) | Informatica IDMC (SaaS) |
| On-prem catalog | Atlas + DataHub (unified) | IBM Watson Knowledge Catalog |
| Federated query | Trino on K8s | IBM Watson Query |
| Privacy enforcement | Apache Ranger | IBM Data Privacy Passports |
| On-prem infra | K8s (any) | IBM Cloud Pak for Data (OpenShift) |
| Ops overhead | Very high | Medium (agents only) |
| IBM dependency | None | Deep |
| Vendor lock-in | None | High (IBM + Collibra) |
| Cost at scale | Infrastructure only | High licensing |

---

## When to Choose This Implementation

✅ Maximum control and zero vendor lock-in
✅ Strong on-prem Kubernetes platform team
✅ Data sovereignty: all metadata and query compute stays on-prem
✅ Existing Apache Ranger / Atlas investment to build on
✅ DataHub preferred over Atlas for modern UX and data products

❌ Small platform team → use 6.6 (managed agents, SaaS control plane)
❌ IBM ecosystem is core → use 6.6
❌ No K8s experience on-prem → use 6.4 or 6.6
❌ Cloud-native primary → use 6.1 / 6.2 / 6.3
