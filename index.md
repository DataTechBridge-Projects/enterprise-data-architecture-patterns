# Enterprise Data Architecture — Master Index

> **Scope:** Enterprise-only. 10 core architectural patterns × 4 variation axes.
> **How to use:** Pick a pattern → choose your axes → get a concrete architecture document.

---

## The 10 Core Patterns

| # | Pattern | The Core Idea | Primary Driver |
|---|---------|---------------|----------------|
| 1 | Enterprise Data Warehouse (EDW) | Centralized, governed, structured, history-preserving analytical store | Reporting & BI at scale |
| 2 | Data Lake | Raw, flexible, schema-on-read repository for all data types | Exploration & cost-efficient storage |
| 3 | Data Lakehouse | Lake economics + warehouse reliability via open table formats | Unified batch + query at scale |
| 4 | Streaming / Event-Driven Data Platform | All data as ordered events; real-time first | Operational intelligence & real-time |
| 5 | Data Mesh | Decentralized, domain-owned data products with federated governance | Scale of teams & domains |
| 6 | Data Fabric | Automated, metadata-driven, virtual integration across hybrid/multi-cloud | Heterogeneous estate unification |
| 7 | Operational Analytics Platform | Analytics on live operational data; near-zero latency (ODS / HTAP) | Operational decisions in-the-moment |
| 8 | AI / ML Data Platform | Feature stores, model lifecycle, vector stores, LLM pipelines | ML/AI production at enterprise scale |
| 9 | Governed / Compliance-First Platform | Privacy, residency, masking, audit as the primary architectural constraint | Regulated industries (GDPR/HIPAA/PCI) |
| 10 | Self-Serve Analytics Engineering Platform | Domain users build with guardrails; semantic layer + metrics + headless BI | Democratized analytics, reduced IT bottleneck |

---

## The 4 Variation Axes

Every pattern is documented across these axes. Each combination = one concrete architecture document.

```
AXIS 1 — Modeling Approach       AXIS 2 — Cloud & Deployment
  ├── Kimball (Star/Snowflake)      ├── AWS-native
  ├── Inmon (3NF + Data Marts)      ├── Azure-native
  ├── Data Vault 2.0                ├── GCP-native
  └── Activity Schema / Wide Table  ├── Multi-Cloud (portable/open)
      (modern analytics engineering) └── Hybrid (on-prem + cloud)

AXIS 3 — Tool Stack               AXIS 4 — Processing Paradigm
  ├── Fully Managed SaaS (Buy)       ├── Batch-first
  ├── Cloud-Native OSS               ├── Streaming-first
  └── Self-Hosted OSS (Full Build)   └── Hybrid (Lambda / Kappa embedded)
```

---

## Cross-Cutting Concerns

| Concern | Applies To |
|---------|-----------|
| Ingestion & Integration (ETL / ELT / CDC / API / Streaming) | All patterns |
| Data Modeling (Kimball / Inmon / Data Vault / Activity Schema) | Patterns 1–3, 5 |
| Orchestration (Airflow / Prefect / Dagster / dbt Cloud) | All patterns |
| Data Quality & Observability (Great Expectations / Monte Carlo / Soda) | All patterns |
| Governance & Catalog (Collibra / Alation / DataHub / Unity Catalog) | All patterns |
| Security (RBAC / ABAC / Column & Row Security / Encryption / KMS) | All patterns |
| DataOps / CI-CD for Data | All patterns |
| FinOps / Cost Management | All patterns |

---

## Currently Documented

### Pattern 02 — Data Lake

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 2.1 | AWS | Fully Managed | S3 + AWS Glue + Lake Formation + Athena |
| 2.2 | AWS | OSS on Cloud | S3 + Spark on EMR + Hive Metastore + Trino |
| 2.3 | Azure | Fully Managed | ADLS Gen2 + ADF + Purview + Synapse Serverless |
| 2.4 | Azure | OSS on Cloud | ADLS + Spark on HDInsight/AKS + Apache Atlas + Trino |
| 2.5 | GCP | Fully Managed | GCS + Dataflow + Dataplex + BigQuery |
| 2.6 | GCP | OSS on Cloud | GCS + Spark on Dataproc + Hive Metastore + Trino |
| 2.7 | Multi-Cloud | OSS Portable | S3/ADLS/GCS + Trino + Apache Atlas + Airflow |
| 2.8 | Hybrid | OSS Self-Hosted | HDFS / MinIO + Hive + Spark + Ranger + Airflow |

### Pattern 03 — Data Lakehouse

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 3.1 | AWS | Fully Managed | S3 + Iceberg + AWS Glue + Redshift Spectrum + dbt Cloud |
| 3.2 | AWS | OSS on Cloud | S3 + Iceberg + Spark on EMR + dbt Core + Trino |
| 3.3 | Azure | Fully Managed | ADLS + Azure Databricks + Delta Lake + dbt Cloud |
| 3.4 | Azure | OSS on Cloud | ADLS + Delta Lake + Spark + dbt Core + Airflow |
| 3.5 | GCP | Fully Managed | GCS + BigLake + Iceberg + dbt Cloud + BigQuery |
| 3.6 | GCP | OSS on Cloud | GCS + Iceberg + Spark on Dataproc + dbt Core + Airflow |
| 3.7 | Multi-Cloud | Fully Managed | Databricks + Delta Lake + dbt Cloud + Tableau |
| 3.8 | Multi-Cloud | OSS Portable | S3/ADLS/GCS + Iceberg + Spark + dbt Core + Trino |
| 3.9 | Hybrid | OSS Self-Hosted | MinIO + Iceberg + Spark on K8s + dbt Core + Airflow |
