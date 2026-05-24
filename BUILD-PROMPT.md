# Build Prompt — Enterprise Data Architecture Patterns

Use this prompt to build any remaining pattern. Replace the variables at the bottom.

---

## Prompt

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. All 8 implementations are in `patterns/02-data-lake/implementations/`. Read any one of them before starting to match the style precisely.

We are building for all list here : C:\Projects\2026-05-24-Data-Architecture-Desgin\architecture-index.md
completed with 02, then go with next

**Now build Pattern {{PATTERN_NUMBER}} — {{PATTERN_NAME}}** following the same structure:

1. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` to internalize the format
2. Create the folder: `patterns/{{PATTERN_FOLDER}}/implementations/`
3. Create one subfolder per implementation listed in the table below
4. Write a `README.md` in each subfolder

**Each README must contain exactly:**
- Header: pattern number, name, cloud, stack, processing type, buy vs build
- ASCII architecture overview (full end-to-end, box drawing characters)
- Mermaid flowchart (`flowchart LR` or `TD`) showing sources → ingestion → storage → catalog → consumption
- Storage zone design (folder/path structure)
- Security model (ASCII table: role → access level → zone)
- Orchestration flow (Mermaid)
- Component map table (component | tool/service | notes)
- Comparison table vs the managed OR previous implementation of the same pattern

**Rules:**
- Consumers connect to the **storage layer** (not the processing layer). The catalog sits between storage and consumers as a schema/access-control layer — show it with dotted lines or as an enabling layer, not a data hop
- Minimal prose — let diagrams carry the content
- No filler text or generic explanations
- Every diagram must name the actual tools/services, not generic labels
- Create all implementation files in parallel

**Implementations to create:**

3--data-lakehouse
---

## Patterns Queue

| Done | Pattern | Folder | Implementations |
|------|---------|--------|-----------------|
| ✅ | 02 — Data Lake | `02-data-lake` | 2.1 AWS Managed · 2.2 AWS OSS · 2.3 Azure Managed · 2.4 Azure OSS · 2.5 GCP Managed · 2.6 GCP OSS · 2.7 Multi-Cloud OSS · 2.8 Hybrid Self-Hosted |
| ⬜ | 01 — Enterprise Data Warehouse | `01-enterprise-data-warehouse` | 1.1 AWS Managed · 1.2 AWS OSS · 1.3 Azure Managed · 1.4 Azure OSS · 1.5 GCP Managed · 1.6 GCP OSS · 1.7 Multi-Cloud Snowflake Managed · 1.8 Multi-Cloud Snowflake OSS · 1.9 Hybrid Managed · 1.10 Hybrid OSS |
| ⬜ | 03 — Data Lakehouse | `03-data-lakehouse` | 3.1 AWS Managed · 3.2 AWS OSS · 3.3 Azure Managed · 3.4 Azure OSS · 3.5 GCP Managed · 3.6 GCP OSS · 3.7 Multi-Cloud Databricks · 3.8 Multi-Cloud OSS Portable · 3.9 Hybrid Self-Hosted |
| ⬜ | 04 — Streaming / Event-Driven | `04-streaming-event-driven` | 4.1 AWS Managed · 4.2 AWS OSS · 4.3 Azure Managed · 4.4 Azure OSS · 4.5 GCP Managed · 4.6 GCP OSS · 4.7 Multi-Cloud Confluent · 4.8 Multi-Cloud OSS Portable · 4.9 Hybrid Self-Hosted |
| ⬜ | 05 — Data Mesh | `05-data-mesh` | 5.1 AWS Managed · 5.2 AWS OSS · 5.3 Azure Managed · 5.4 Azure OSS · 5.5 GCP Managed · 5.6 GCP OSS · 5.7 Multi-Cloud Databricks · 5.8 Multi-Cloud OSS Portable |
| ⬜ | 06 — Data Fabric | `06-data-fabric` | 6.1 AWS Managed · 6.2 Azure Managed · 6.3 GCP Managed · 6.4 Multi-Cloud Managed · 6.5 Multi-Cloud OSS · 6.6 Hybrid Managed · 6.7 Hybrid OSS Self-Hosted |
| ⬜ | 07 — Operational Analytics | `07-operational-analytics` | 7.1 AWS Managed · 7.2 AWS OSS · 7.3 Azure Managed · 7.4 Azure OSS · 7.5 GCP Managed · 7.6 GCP OSS · 7.7 Multi-Cloud Managed · 7.8 Hybrid Self-Hosted |
| ⬜ | 08 — AI / ML Data Platform | `08-ai-ml-platform` | 8.1 AWS Managed · 8.2 AWS OSS · 8.3 Azure Managed · 8.4 Azure OSS · 8.5 GCP Managed · 8.6 GCP OSS · 8.7 Multi-Cloud Databricks · 8.8 Multi-Cloud OSS Portable · 8.9 Hybrid Self-Hosted |
| ⬜ | 09 — Governed / Compliance-First | `09-governed-compliance` | 9.1 AWS Managed · 9.2 AWS OSS · 9.3 Azure Managed · 9.4 Azure OSS · 9.5 GCP Managed · 9.6 GCP OSS · 9.7 Multi-Cloud Snowflake · 9.8 Multi-Cloud OSS · 9.9 Hybrid Managed · 9.10 Hybrid OSS Self-Hosted |
| ⬜ | 10 — Self-Serve Analytics Engineering | `10-self-serve-analytics` | 10.1 AWS Managed · 10.2 AWS OSS · 10.3 Azure Managed · 10.4 Azure OSS · 10.5 GCP Managed · 10.6 GCP OSS · 10.7 Multi-Cloud Snowflake · 10.8 Multi-Cloud OSS Portable · 10.9 Hybrid Self-Hosted |

---

## Pre-filled Prompts Per Pattern

Copy the entire block for the pattern you want to build.

---

### Pattern 01 — Enterprise Data Warehouse

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 01 — Enterprise Data Warehouse. Create `patterns/01-enterprise-data-warehouse/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 1.1 | `1.1-aws-managed` | AWS | Redshift + AWS Glue + dbt Cloud + QuickSight | Fully Managed |
| 1.2 | `1.2-aws-oss` | AWS | Redshift + Airbyte + dbt Core + Airflow + Superset | OSS on Cloud |
| 1.3 | `1.3-azure-managed` | Azure | Synapse Analytics + ADF + dbt Cloud + Power BI | Fully Managed |
| 1.4 | `1.4-azure-oss` | Azure | Synapse + Airbyte + dbt Core + Airflow + Superset | OSS on Cloud |
| 1.5 | `1.5-gcp-managed` | GCP | BigQuery + Datastream + dbt Cloud + Looker | Fully Managed |
| 1.6 | `1.6-gcp-oss` | GCP | BigQuery + Airbyte + dbt Core + Airflow + Superset | OSS on Cloud |
| 1.7 | `1.7-multicloud-snowflake-managed` | Multi-Cloud | Snowflake + Fivetran + dbt Cloud + Tableau | Fully Managed |
| 1.8 | `1.8-multicloud-snowflake-oss` | Multi-Cloud | Snowflake + Airbyte + dbt Core + Airflow + Metabase | OSS on Cloud |
| 1.9 | `1.9-hybrid-managed` | Hybrid | Teradata Vantage / IBM Db2 Warehouse + Informatica | Fully Managed |
| 1.10 | `1.10-hybrid-oss` | Hybrid | PostgreSQL + dbt Core + Airbyte + Airflow | OSS Self-Hosted |

Follow all rules from the template: consumers connect to the storage/warehouse layer via catalog, not to the processing layer. Minimal prose, diagrams carry the content, actual tool names in every diagram.

---

### Pattern 03 — Data Lakehouse

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 03 — Data Lakehouse. Create `patterns/03-data-lakehouse/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 3.1 | `3.1-aws-managed` | AWS | S3 + Iceberg + AWS Glue + Redshift Spectrum + dbt Cloud | Fully Managed |
| 3.2 | `3.2-aws-oss` | AWS | S3 + Iceberg + Spark on EMR + dbt Core + Airflow + Trino | OSS on Cloud |
| 3.3 | `3.3-azure-managed` | Azure | ADLS + Delta Lake + Databricks + dbt Cloud + Synapse | Fully Managed |
| 3.4 | `3.4-azure-oss` | Azure | ADLS + Delta Lake + Spark on AKS + dbt Core + Airflow | OSS on Cloud |
| 3.5 | `3.5-gcp-managed` | GCP | GCS + BigLake + Iceberg + dbt Cloud + BigQuery | Fully Managed |
| 3.6 | `3.6-gcp-oss` | GCP | GCS + Iceberg + Spark on Dataproc + dbt Core + Airflow | OSS on Cloud |
| 3.7 | `3.7-multicloud-databricks` | Multi-Cloud | Databricks + Delta Lake + dbt Cloud + Tableau | Fully Managed |
| 3.8 | `3.8-multicloud-oss-portable` | Multi-Cloud | Iceberg + Spark + dbt Core + Airflow + Trino | OSS Portable |
| 3.9 | `3.9-hybrid-selfhosted` | Hybrid | MinIO + Iceberg + Spark on K8s + dbt Core + Airflow | Self-Hosted |

Follow all rules from the template. Key difference from Data Lake: show ACID transactions, time travel, and the open table format (Delta/Iceberg) as a distinct layer between raw storage and the query engine.

---

### Pattern 04 — Streaming / Event-Driven Data Platform

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 04 — Streaming / Event-Driven Data Platform. Create `patterns/04-streaming-event-driven/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 4.1 | `4.1-aws-managed` | AWS | Kinesis + MSK + Flink on KDA + DynamoDB / Redshift | Fully Managed |
| 4.2 | `4.2-aws-oss` | AWS | Kafka on MSK + Apache Flink on EMR + Iceberg | OSS on Cloud |
| 4.3 | `4.3-azure-managed` | Azure | Event Hubs + Stream Analytics + Cosmos DB + Synapse | Fully Managed |
| 4.4 | `4.4-azure-oss` | Azure | Event Hubs (Kafka API) + Flink on AKS + Delta Lake | OSS on Cloud |
| 4.5 | `4.5-gcp-managed` | GCP | Pub/Sub + Dataflow (Beam) + BigQuery Streaming + Bigtable | Fully Managed |
| 4.6 | `4.6-gcp-oss` | GCP | Pub/Sub + Flink on GKE + Iceberg on GCS | OSS on Cloud |
| 4.7 | `4.7-multicloud-confluent` | Multi-Cloud | Confluent Cloud + Flink Cloud + Snowflake Streaming | Fully Managed |
| 4.8 | `4.8-multicloud-oss-portable` | Multi-Cloud | Apache Kafka + Apache Flink + Iceberg + ksqlDB | OSS Portable |
| 4.9 | `4.9-hybrid-selfhosted` | Hybrid | Kafka on K8s + Flink on K8s + Cassandra + MinIO | Self-Hosted |

Key difference: the architecture is stream-first. Show the event broker (Kafka/Kinesis/Pub-Sub) as the central spine. Show both the stream processing layer and the serving store (hot path) separately from any lake/warehouse (cold path).

---

### Pattern 05 — Data Mesh

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 05 — Data Mesh. Create `patterns/05-data-mesh/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 5.1 | `5.1-aws-managed` | AWS | S3 + Lake Formation + Glue Catalog + Redshift + DataZone | Fully Managed |
| 5.2 | `5.2-aws-oss` | AWS | S3 + Iceberg + Trino + DataHub + dbt Core + Airflow | OSS on Cloud |
| 5.3 | `5.3-azure-managed` | Azure | ADLS + Purview + Synapse + dbt Cloud (per domain) | Fully Managed |
| 5.4 | `5.4-azure-oss` | Azure | ADLS + Delta Lake + DataHub + dbt Core + Airflow | OSS on Cloud |
| 5.5 | `5.5-gcp-managed` | GCP | GCS + Dataplex + BigQuery + dbt Cloud (per domain) | Fully Managed |
| 5.6 | `5.6-gcp-oss` | GCP | GCS + Iceberg + DataHub + dbt Core + Airflow | OSS on Cloud |
| 5.7 | `5.7-multicloud-databricks` | Multi-Cloud | Databricks Unity Catalog + dbt Cloud + Alation | Fully Managed |
| 5.8 | `5.8-multicloud-oss-portable` | Multi-Cloud | Iceberg + Nessie + DataHub + dbt Core + Airflow | OSS Portable |

Key difference: show the domain boundary clearly. Each domain owns its own storage + pipeline + data product output. The platform layer (catalog, governance, self-serve infra) is shared. Show data products as the interface between domains and consumers — not raw storage.

---

### Pattern 06 — Data Fabric

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 06 — Data Fabric. Create `patterns/06-data-fabric/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 6.1 | `6.1-aws-managed` | AWS | Glue + Lake Formation + Athena Federation + Macie + DataZone | Fully Managed |
| 6.2 | `6.2-azure-managed` | Azure | Purview + Synapse Link + API Management + Defender | Fully Managed |
| 6.3 | `6.3-gcp-managed` | GCP | Dataplex + BigQuery Omni + Data Catalog + Chronicle | Fully Managed |
| 6.4 | `6.4-multicloud-managed` | Multi-Cloud | Informatica IDMC + Collibra + Talend + Starburst | Fully Managed |
| 6.5 | `6.5-multicloud-oss` | Multi-Cloud | Apache Atlas + Trino + OpenMetadata + Apache Ranger | OSS Portable |
| 6.6 | `6.6-hybrid-managed` | Hybrid | Informatica IDMC + Collibra + IBM Cloud Pak for Data | Fully Managed |
| 6.7 | `6.7-hybrid-oss-selfhosted` | Hybrid | Apache Atlas + Trino + Apache Ranger + DataHub | Self-Hosted |

Key difference: Data Fabric does NOT physically move data. Show the metadata layer (active metadata) as the control plane. Show virtual/federated query as the primary access mechanism. Physical data stays in-place — the fabric provides a unified logical view.

---

### Pattern 07 — Operational Analytics Platform

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 07 — Operational Analytics Platform. Create `patterns/07-operational-analytics/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 7.1 | `7.1-aws-managed` | AWS | Aurora + DMS CDC + Redshift ODS + QuickSight | Fully Managed |
| 7.2 | `7.2-aws-oss` | AWS | PostgreSQL + Debezium + Kafka + Flink + Iceberg + Superset | OSS on Cloud |
| 7.3 | `7.3-azure-managed` | Azure | Azure SQL + Synapse Link + Cosmos DB Analytical + Power BI | Fully Managed |
| 7.4 | `7.4-azure-oss` | Azure | PostgreSQL + Debezium + Event Hubs + Delta Lake + Superset | OSS on Cloud |
| 7.5 | `7.5-gcp-managed` | GCP | Cloud Spanner + Datastream CDC + BigQuery + Looker | Fully Managed |
| 7.6 | `7.6-gcp-oss` | GCP | PostgreSQL + Debezium + Pub/Sub + Iceberg + Superset | OSS on Cloud |
| 7.7 | `7.7-multicloud-managed` | Multi-Cloud | CockroachDB / SingleStore + Fivetran + Snowflake + Tableau | Fully Managed |
| 7.8 | `7.8-hybrid-selfhosted` | Hybrid | PostgreSQL + Debezium + Kafka + Apache Pinot / Druid + Superset | Self-Hosted |

Key difference: latency is the primary constraint (seconds, not hours). Show the CDC path from OLTP → ODS/HTAP store clearly. Distinguish between the operational store (sub-minute latency) and any downstream analytical store (minutes/hours). Show both hot-path and cold-path consumers.

---

### Pattern 08 — AI / ML Data Platform

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 08 — AI / ML Data Platform. Create `patterns/08-ai-ml-platform/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 8.1 | `8.1-aws-managed` | AWS | SageMaker Feature Store + SageMaker Pipelines + OpenSearch Vector + Bedrock | Fully Managed |
| 8.2 | `8.2-aws-oss` | AWS | Feast + MLflow + Airflow + Qdrant + LangChain | OSS on Cloud |
| 8.3 | `8.3-azure-managed` | Azure | Azure ML Feature Store + Azure ML + AI Search + Azure OpenAI | Fully Managed |
| 8.4 | `8.4-azure-oss` | Azure | Feast + MLflow + Airflow + Qdrant + LangChain on AKS | OSS on Cloud |
| 8.5 | `8.5-gcp-managed` | GCP | Vertex AI Feature Store + Vertex Pipelines + Vertex Vector Search + Gemini | Fully Managed |
| 8.6 | `8.6-gcp-oss` | GCP | Feast + MLflow + Kubeflow + Weaviate on GKE + LangChain | OSS on Cloud |
| 8.7 | `8.7-multicloud-databricks` | Multi-Cloud | Databricks Feature Store + MLflow + Vector Search + dbt Cloud | Fully Managed |
| 8.8 | `8.8-multicloud-oss-portable` | Multi-Cloud | Feast + MLflow + Airflow + Qdrant + LangChain + dbt Core | OSS Portable |
| 8.9 | `8.9-hybrid-selfhosted` | Hybrid | Feast + MLflow + Kubeflow + Qdrant on K8s + Ollama | Self-Hosted |

Key difference: show two distinct data paths — (1) offline path: batch feature computation → offline store → model training; (2) online path: low-latency feature serving → online store → real-time inference. Also show the vector store + embedding pipeline as a separate branch for RAG/LLM use cases.

---

### Pattern 09 — Governed / Compliance-First Platform

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 09 — Governed / Compliance-First Platform. Create `patterns/09-governed-compliance/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 9.1 | `9.1-aws-managed` | AWS | Redshift + Lake Formation RBAC/ABAC + Macie + CloudTrail + KMS | Fully Managed |
| 9.2 | `9.2-aws-oss` | AWS | S3 + Iceberg + Apache Ranger + OpenMetadata + Debezium audit | OSS on Cloud |
| 9.3 | `9.3-azure-managed` | Azure | Synapse + Purview + Azure Policy + Key Vault + Defender | Fully Managed |
| 9.4 | `9.4-azure-oss` | Azure | ADLS + Delta Lake + Apache Ranger + Collibra + Key Vault | OSS on Cloud |
| 9.5 | `9.5-gcp-managed` | GCP | BigQuery + Data Catalog + DLP API + Cloud KMS + Dataplex | Fully Managed |
| 9.6 | `9.6-gcp-oss` | GCP | GCS + Iceberg + Apache Ranger + OpenMetadata + Cloud KMS | OSS on Cloud |
| 9.7 | `9.7-multicloud-snowflake` | Multi-Cloud | Snowflake RBAC + Dynamic Masking + Data Clean Room + Collibra | Fully Managed |
| 9.8 | `9.8-multicloud-oss` | Multi-Cloud | Iceberg + Apache Ranger + OpenMetadata + HashiCorp Vault + Trino | OSS Portable |
| 9.9 | `9.9-hybrid-managed` | Hybrid | Informatica IDMC + Collibra + IBM Guardium + Protegrity | Fully Managed |
| 9.10 | `9.10-hybrid-oss-selfhosted` | Hybrid | Apache Atlas + Ranger + HashiCorp Vault + OpenMetadata | Self-Hosted |

Key difference: compliance is the architecture driver, not an afterthought. Show the classification layer (PII/PHI/PCI tagging) as a mandatory step on ingest. Show encryption, masking, and access control as distinct labeled layers. Include a data flow for right-to-be-forgotten / data deletion propagation.

---

### Pattern 10 — Self-Serve Analytics Engineering Platform

We are building an enterprise data architecture reference library. Pattern 02 (Data Lake) is fully built and is the template to follow exactly. Read `patterns/02-data-lake/implementations/2.1-aws-managed/README.md` before starting.

Build Pattern 10 — Self-Serve Analytics Engineering Platform. Create `patterns/10-self-serve-analytics/implementations/` and one subfolder + README.md per row below. Create all files in parallel.

| # | Folder | Cloud | Stack | Type |
|---|--------|-------|-------|------|
| 10.1 | `10.1-aws-managed` | AWS | Redshift + dbt Cloud + AtScale / Cube Cloud + Tableau | Fully Managed |
| 10.2 | `10.2-aws-oss` | AWS | Redshift / Trino + dbt Core + Cube.js + Superset + Airflow | OSS on Cloud |
| 10.3 | `10.3-azure-managed` | Azure | Synapse + dbt Cloud + Power BI Semantic Model + AtScale | Fully Managed |
| 10.4 | `10.4-azure-oss` | Azure | Synapse / DuckDB + dbt Core + Cube.js + Superset + Airflow | OSS on Cloud |
| 10.5 | `10.5-gcp-managed` | GCP | BigQuery + dbt Cloud + Looker (LookML semantic layer) | Fully Managed |
| 10.6 | `10.6-gcp-oss` | GCP | BigQuery + dbt Core + Cube.js + Superset + Airflow | OSS on Cloud |
| 10.7 | `10.7-multicloud-snowflake` | Multi-Cloud | Snowflake + dbt Cloud + Tableau + dbt Semantic Layer | Fully Managed |
| 10.8 | `10.8-multicloud-oss-portable` | Multi-Cloud | DuckDB / Trino + dbt Core + Cube.js + Superset + Airflow | OSS Portable |
| 10.9 | `10.9-hybrid-selfhosted` | Hybrid | PostgreSQL + dbt Core + Cube.js + Superset + Airflow on K8s | Self-Hosted |

Key difference: the semantic/metric layer is the central component — show it explicitly between the warehouse/lakehouse and the BI tools. Show the dbt transformation layer building the Gold/mart models. Show how business users interact with the semantic layer (not the raw warehouse). Include a diagram showing the developer workflow (git → dbt CI → warehouse → semantic layer).
