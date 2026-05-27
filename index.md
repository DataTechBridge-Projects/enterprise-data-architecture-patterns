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

### Snowflake Data Cloud

| # | Implementation | Stack | Key Tools |
|---|---------------|-------|-----------|
| SF.1 | Classic EDW & BI | Fully Managed | Fivetran + Snowflake + dbt Cloud + Tableau / Power BI |
| SF.2 | Snowflake Lakehouse | Hybrid | Airbyte + Snowflake + Apache Iceberg + dbt Core + Airflow |
| SF.3 | AI / ML (Snowpark + Cortex) | Fully Managed | Fivetran + Snowflake + Snowpark ML + Cortex AI + Streamlit |
| SF.4 | Governed & Compliance | Fully Managed | Fivetran + Snowflake DDM + Row Access Policies + Clean Room + Collibra |

### Databricks Lakehouse Platform

| # | Implementation | Stack | Key Tools |
|---|---------------|-------|-----------|
| DB.1 | Delta Lakehouse | Fully Managed | Autoloader + Delta Live Tables + dbt Cloud + Databricks SQL + Tableau |
| DB.2 | Streaming & Real-Time | Fully Managed | Kafka + Structured Streaming + Delta Live Tables + Delta Lake |
| DB.3 | AI / ML Platform | Fully Managed | Databricks + MLflow + Feature Store + Vector Search + LangChain |
| DB.4 | Data Mesh (Unity Catalog) | Fully Managed | Unity Catalog + domain workspaces + dbt Cloud + Collibra |

### Pattern 01 — Enterprise Data Warehouse

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 1.1 | AWS | Fully Managed | Redshift + AWS Glue + dbt Cloud + QuickSight / Tableau |
| 1.2 | AWS | OSS on Cloud | Redshift + Airbyte + dbt Core + Airflow + Superset |
| 1.3 | Azure | Fully Managed | Synapse Analytics + ADF + dbt Cloud + Power BI |
| 1.4 | Azure | OSS on Cloud | Synapse + Airbyte + dbt Core + Airflow + Superset |
| 1.5 | GCP | Fully Managed | BigQuery + Datastream + dbt Cloud + Looker |
| 1.6 | GCP | OSS on Cloud | BigQuery + Airbyte + dbt Core + Airflow + Superset |
| 1.7 | Multi-Cloud | Fully Managed | Snowflake + Fivetran + dbt Cloud + Tableau |
| 1.8 | Multi-Cloud | OSS on Cloud | Snowflake + Airbyte + dbt Core + Airflow + Metabase |

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

### Pattern 04 — Streaming / Event-Driven Data Platform

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 4.1 | AWS | Fully Managed | Kinesis + MSK + Flink on KDA + DynamoDB / Redshift |
| 4.2 | AWS | OSS on Cloud | Kafka on MSK + Apache Flink on EMR + Cassandra / Iceberg |
| 4.3 | Azure | Fully Managed | Event Hubs + Azure Stream Analytics + Cosmos DB + Synapse |
| 4.4 | Azure | OSS on Cloud | Event Hubs (Kafka API) + Apache Flink on AKS + Delta Lake |
| 4.5 | GCP | Fully Managed | Pub/Sub + Dataflow (Beam) + BigQuery Streaming + Bigtable |
| 4.6 | GCP | OSS on Cloud | Pub/Sub + Apache Flink on GKE + Iceberg on GCS |
| 4.7 | Multi-Cloud | Fully Managed | Confluent Cloud + Flink Cloud + Snowflake Streaming |
| 4.8 | Multi-Cloud | OSS Portable | Apache Kafka + Apache Flink + Apache Iceberg + ksqlDB |
| 4.9 | Hybrid | OSS Self-Hosted | Kafka on K8s + Flink on K8s + Cassandra + MinIO |

### Pattern 05 — Data Mesh

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 5.1 | AWS | Fully Managed | S3 + Lake Formation + Glue Catalog + Redshift + AWS DataZone |
| 5.2 | AWS | OSS on Cloud | S3 + Iceberg + Trino + DataHub + dbt Core + Airflow |
| 5.3 | Azure | Fully Managed | ADLS + Purview + Synapse (per domain) + ADF + dbt Cloud |
| 5.4 | Azure | OSS on Cloud | ADLS + Delta Lake + DataHub + dbt Core + Airflow on AKS |
| 5.5 | GCP | Fully Managed | GCS + Dataplex + BigQuery (per domain) + Analytics Hub + dbt Cloud |
| 5.6 | GCP | OSS on Cloud | GCS + Iceberg + DataHub + dbt Core + Trino on GKE + Airflow |
| 5.7 | Multi-Cloud | Fully Managed | Databricks Unity Catalog + Delta Lake + dbt Cloud + Collibra |
| 5.8 | Multi-Cloud | OSS Portable | Iceberg + Nessie Catalog + DataHub + OpenMetadata + dbt Core + Trino |

### Pattern 06 — Data Fabric

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 6.1 | AWS | Fully Managed | AWS Glue + Lake Formation + Athena Federation + Macie + DataZone |
| 6.2 | Azure | Fully Managed | Azure Purview + Synapse Link + Azure API Management + Defender |
| 6.3 | GCP | Fully Managed | Dataplex + BigQuery Omni + Data Catalog + Chronicle |
| 6.4 | Multi-Cloud | Fully Managed | Informatica IDMC + Collibra + Talend + Starburst |
| 6.5 | Multi-Cloud | OSS Portable | Apache Atlas + Trino + OpenMetadata + Apache Ranger |
| 6.6 | Hybrid | Fully Managed | Informatica IDMC + Collibra + IBM Cloud Pak for Data |
| 6.7 | Hybrid | OSS Self-Hosted | Apache Atlas + Trino + Apache Ranger + DataHub |

### Pattern 07 — Operational Analytics Platform

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 7.1 | AWS | Fully Managed | Aurora + DMS (CDC) + Redshift (ODS) + QuickSight |
| 7.2 | AWS | OSS on Cloud | PostgreSQL + Debezium + Kafka + Flink + Iceberg + Superset |
| 7.3 | Azure | Fully Managed | Azure SQL + Synapse Link + Cosmos DB Analytical Store + Power BI |
| 7.4 | Azure | OSS on Cloud | PostgreSQL + Debezium + Event Hubs + Delta Lake + Superset |
| 7.5 | GCP | Fully Managed | Cloud Spanner + Datastream (CDC) + BigQuery + Looker |
| 7.6 | GCP | OSS on Cloud | PostgreSQL + Debezium + Pub/Sub + Iceberg + Superset |
| 7.7 | Multi-Cloud | Fully Managed | CockroachDB / SingleStore + Fivetran + Snowflake + Tableau |
| 7.8 | Hybrid | OSS Self-Hosted | PostgreSQL + Debezium + Kafka + Apache Pinot / Druid + Superset |

### Pattern 08 — AI / ML Data Platform

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 8.1 | AWS | Fully Managed | SageMaker Feature Store + SageMaker Pipelines + OpenSearch (vector) + Bedrock |
| 8.2 | AWS | OSS on Cloud | Feast + MLflow + Airflow + Qdrant / Weaviate + LangChain |
| 8.3 | Azure | Fully Managed | Azure ML Feature Store + Azure ML + Azure AI Search + Azure OpenAI |
| 8.4 | Azure | OSS on Cloud | Feast + MLflow + Airflow + Qdrant + LangChain on AKS |
| 8.5 | GCP | Fully Managed | Vertex AI Feature Store + Vertex Pipelines + Vertex Vector Search + Gemini |
| 8.6 | GCP | OSS on Cloud | Feast + MLflow + Kubeflow + Weaviate on GKE + LangChain |
| 8.7 | Multi-Cloud | Fully Managed | Databricks Feature Store + MLflow + Vector Search + dbt Cloud |
| 8.8 | Multi-Cloud | OSS Portable | Feast + MLflow + Airflow + Qdrant + LangChain + dbt Core |
| 8.9 | Hybrid | OSS Self-Hosted | Feast + MLflow + Kubeflow + Qdrant on K8s + Ollama |

### Pattern 09 — Governed / Compliance-First Platform

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 9.1 | AWS | Fully Managed | Redshift + Lake Formation (RBAC/ABAC) + Macie + CloudTrail + AWS KMS |
| 9.2 | AWS | OSS on Cloud | S3 + Iceberg + Apache Ranger + OpenMetadata + Debezium (audit) |
| 9.3 | Azure | Fully Managed | Synapse + Purview + Azure Policy + Key Vault + Microsoft Defender |
| 9.4 | Azure | OSS on Cloud | ADLS + Delta Lake + Apache Ranger + Collibra + Azure Key Vault |
| 9.5 | GCP | Fully Managed | BigQuery + Data Catalog + DLP API + Cloud KMS + Dataplex |
| 9.6 | GCP | OSS on Cloud | GCS + Iceberg + Apache Ranger + OpenMetadata + Cloud KMS |
| 9.7 | Multi-Cloud | Fully Managed | Snowflake RBAC + Dynamic Masking + Data Clean Room + Collibra + Fivetran |
| 9.8 | Multi-Cloud | OSS Portable | Iceberg + Apache Ranger + OpenMetadata + HashiCorp Vault + Trino |
| 9.9 | Hybrid | Fully Managed | Informatica IDMC + Collibra + IBM Guardium + Protegrity |
| 9.10 | Hybrid | OSS Self-Hosted | Apache Atlas + Ranger + HashiCorp Vault + OpenMetadata + Audit Sinks |

### Pattern 10 — Self-Serve Analytics Engineering Platform

| # | Cloud | Stack | Key Tools |
|---|-------|-------|-----------|
| 10.1 | AWS | Fully Managed | Redshift + dbt Cloud + AtScale / Cube Cloud + Tableau / Looker |
| 10.2 | AWS | OSS on Cloud | Redshift / Trino + dbt Core + Cube.js + Superset + Airflow |
| 10.3 | Azure | Fully Managed | Synapse + dbt Cloud + Power BI (semantic model) + AtScale |
| 10.4 | Azure | OSS on Cloud | Synapse / DuckDB + dbt Core + Cube.js + Superset + Airflow |
| 10.5 | GCP | Fully Managed | BigQuery + dbt Cloud + Looker (LookML semantic layer) |
| 10.6 | GCP | OSS on Cloud | BigQuery + dbt Core + Cube.js + Superset + Airflow |
| 10.7 | Multi-Cloud | Fully Managed | Snowflake + dbt Cloud + Tableau + AtScale / dbt Semantic Layer |
| 10.8 | Multi-Cloud | OSS Portable | DuckDB / Trino + dbt Core + Cube.js + Superset + Airflow |
| 10.9 | Hybrid | OSS Self-Hosted | PostgreSQL + dbt Core + Cube.js + Superset + Airflow on K8s |
