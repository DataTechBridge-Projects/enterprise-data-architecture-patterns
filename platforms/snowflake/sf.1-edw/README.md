---
layout: default
title: "sf.1 — Snowflake · Classic EDW & BI"
---

# sf.1 — Snowflake · Classic EDW & BI

**Stack:** Fivetran · Snowflake · dbt Cloud · Tableau / Power BI
**Processing:** Batch-first · Schema-on-Write
**Buy vs Build:** Buy (fully managed SaaS end-to-end)

---

## Architecture Overview

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'fontSize': '18px'}}}%%
flowchart TD
    subgraph SRC["Data Sources"]
        S1[ERP / OLTP\nSAP · Oracle · SQL Server]
        S2[SaaS Applications\nSalesforce · Workday · NetSuite]
        S3[Files and Feeds\nCSV · JSON · Parquet]
        S4[Event Streams\nClickstream · App Events]
    end

    subgraph INGEST["Ingestion — Fivetran"]
        I1[Fivetran Connectors\n300+ pre-built SaaS sources]
        I2[Fivetran HVR\nDB CDC for OLTP]
        I3[Fivetran Batch\nFile-based ingestion]
    end

    subgraph STAGING["Staging Schema — Snowflake"]
        ST1[RAW Database\nfivetran-managed schemas]
        ST2[STG Database\ndbt staging models]
    end

    subgraph EDW["EDW — Snowflake"]
        E1[Integration Layer\nData Vault Hubs · Links · Satellites]
        E2[Dimensional Layer\nKimball Star Schemas]
        E3[Data Marts\nFinance · Sales · HR · Ops]
    end

    subgraph TRANSFORM["Transformation — dbt Cloud"]
        T1[dbt Staging Models\ntype-cast · rename · de-dup]
        T2[dbt Core Models\nconform · integrate]
        T3[dbt Mart Models\nbusiness aggregations]
        T4[dbt Tests\ndata quality assertions]
    end

    subgraph CONSUME["Consumption"]
        F1[Tableau\nEnterprise BI · Dashboards]
        F2[Power BI\nMicrosoft Ecosystem BI]
        F3[Snowflake Worksheets\nAd-hoc SQL]
        F4[Excel via ODBC\nFinance self-service]
    end

    SRC --> INGEST
    INGEST --> ST1
    ST1 --> T1 --> ST2
    ST2 --> T2 --> E1
    E1 --> T2 --> E2
    E2 --> T3 --> E3
    E3 --> T4
    E3 --> F1
    E3 --> F2
    E2 --> F3
    E3 --> F4
```

---

## Data Flow

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'fontSize': '18px'}}}%%
flowchart LR
    subgraph Sources
        A1[(ERP / OLTP\nSAP · Oracle)]
        A2[SaaS APIs\nSalesforce · Workday]
        A3[Files / Feeds]
        A4[Clickstream Events]
    end

    subgraph Fivetran["Fivetran — Managed Ingestion"]
        B1[HVR CDC\nDB replication]
        B2[SaaS Connectors\nAPI-based sync]
        B3[File Connectors\nBatch upload]
    end

    subgraph Raw["RAW Database — Snowflake"]
        C1[raw.salesforce\nraw.sap · raw.workday]
        C2[raw.events\nraw.files]
    end

    subgraph Transform["dbt Cloud — Transformation"]
        D1[Staging Models\nstg_* · conform types]
        D2[Integration Models\ncore_* · Data Vault]
        D3[Mart Models\nmart_* · star schema]
    end

    subgraph Serve["Presentation — Snowflake"]
        E1[dim_* / fact_* tables]
        E2[mart_finance · mart_sales]
        E3[mart_hr · mart_ops]
    end

    subgraph Consume
        F1[Tableau\nEnterprise Dashboards]
        F2[Power BI\nMicrosoft Reports]
        F3[Ad-hoc SQL]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C2
    A4 --> B3 --> C2

    C1 -->|dbt staging| D1
    C2 -->|dbt staging| D1
    D1 -->|dbt core| D2
    D2 -->|dbt mart| D3
    D3 --> E1 --> E2 --> F1
    E2 --> F2
    E3 --> F1
    E1 --> F3
```

---

## Component Breakdown

| Layer | Tool | Role |
|-------|------|------|
| Ingestion — SaaS | Fivetran | 300+ managed connectors; auto-schema migration; SLA-backed sync |
| Ingestion — DB CDC | Fivetran HVR | Log-based CDC from Oracle, SQL Server, SAP HANA |
| Cloud Data Warehouse | Snowflake | Multi-cluster warehouse; separate compute per workload; zero-copy cloning |
| Staging Layer | Snowflake RAW database | Fivetran-managed schemas; immutable raw copy before any transformation |
| Transformation | dbt Cloud | Managed dbt runs; CI/CD for models; lineage + docs auto-generated |
| Orchestration | dbt Cloud Jobs | Scheduled and event-triggered dbt job runs; Slack + email alerts |
| Data Quality | dbt Tests | Schema tests + custom SQL assertions on every mart build |
| Enterprise BI | Tableau | Live and extract connections to Snowflake; row-level security via groups |
| Microsoft BI | Power BI | DirectQuery or import mode via Snowflake connector |
| Access Control | Snowflake RBAC | Role hierarchy: SYSADMIN → functional roles → analyst roles |
| Monitoring | Snowflake Query History + dbt Cloud | Cost per query, job run history, model freshness |

---

## Key Design Decisions

- **Single copy of raw data.** Fivetran lands data into a dedicated RAW database that dbt reads but never writes to, preserving an auditable source-of-truth before any transformation touches the data.
- **Layered dbt project.** Three-tier model structure (staging → integration → mart) keeps transformation logic composable, testable, and independently deployable per domain.
- **Workload isolation via virtual warehouses.** Ingestion, dbt transformation, and BI queries each run on separate Snowflake warehouses, preventing resource contention and enabling per-workload cost tracking.
- **Schema-on-write discipline.** All tables in the dimensional and mart layers have enforced column types and dbt schema contracts, giving BI tools stable, reliable metadata.
- **dbt Cloud as the sole orchestration surface.** Keeping all transformation scheduling inside dbt Cloud eliminates a separate Airflow deployment and reduces operational complexity for teams without platform engineering resources.

---

## When to Choose This Implementation

- Your organisation is standardising on a fully managed, low-ops analytics stack and wants to minimise infrastructure ownership.
- The majority of data sources are SaaS applications (Salesforce, Workday, NetSuite) where Fivetran's pre-built connectors eliminate custom connector engineering.
- BI consumers are primarily power users working in Tableau or Power BI, requiring a stable, well-modelled dimensional layer rather than raw query access.
- The team has strong SQL and dbt skills but limited platform engineering capacity to operate schedulers, container orchestration, or self-hosted tools.

---

## Trade-offs

| Strength | Limitation |
|----------|------------|
| Fastest time-to-value — Fivetran + dbt Cloud can be production-ready in days | Highest SaaS licensing cost; Fivetran MAR-based pricing escalates with row volume |
| Zero infrastructure to manage; Fivetran and dbt Cloud handle upgrades, scaling, and reliability | Limited customisation of ingestion logic; Fivetran connectors are opinionated and not forkable |
| dbt Cloud lineage, docs, and CI/CD are first-class out of the box | dbt Cloud job scheduling is less flexible than Airflow DAGs for complex cross-system dependencies |
| Snowflake's compute-storage separation means BI queries never compete with ETL loads | Snowflake credit consumption can spike unpredictably without warehouse auto-suspend tuning |
| Strong Tableau and Power BI ecosystem with native Snowflake connectors and SSO support | Near-real-time requirements below 15-minute latency require adding Snowpipe or a separate streaming path |
