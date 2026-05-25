---
layout: default
title: "8.2 — AI/ML Data Platform · AWS OSS on Cloud"
---

# 8.2 — AI/ML Data Platform · AWS OSS on Cloud

**Stack:** Feast · MLflow · Apache Airflow (MWAA) · Qdrant · LangChain · S3 · Redis  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Build (OSS on AWS-managed infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[RDS / PostgreSQL]
        S2[S3 Data Lake\nRaw / Curated]
        S3[Kafka / MSK\nEvent Streams]
    end

    subgraph INGEST["Feature Engineering"]
        I1[Airflow DAG\nBatch Feature Jobs]
        I2[Spark on EMR\nLarge-scale Transform]
        I3[Flink / Lambda\nStream Feature Compute]
    end

    subgraph FEATURE["Feature Store — Feast"]
        F1[Offline Store\nS3 + Athena]
        F2[Online Store\nRedis ElastiCache · ms lookup]
        F3[Feature Registry\nPostgreSQL metadata]
    end

    subgraph TRAIN["Training — MLflow + Airflow"]
        T1[Airflow Pipeline\nOrchestrate Training]
        T2[Training Job\nSpark/EMR or EC2 GPU]
        T3[MLflow Tracking\nMetrics · Params · Artifacts]
        T4[MLflow Registry\nStaging → Production]
    end

    subgraph VECTOR["Vector / RAG Layer"]
        V1[Embedding Jobs\nHuggingFace / OpenAI API]
        V2[Qdrant\nVector DB on EC2/EKS]
        V3[LangChain\nRAG Orchestration]
    end

    subgraph SERVE["Serving"]
        E1[BentoML / FastAPI\nReal-Time Endpoint]
        E2[Batch Scoring\nSpark on EMR]
        E3[API Gateway + NLB\nTraffic Routing]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Predictions]
        C2[Analytics\nBatch Scores]
        C3[LLM App\nRAG Responses]
    end

    SRC --> INGEST
    I1 & I2 --> F1
    I3 --> F2
    F1 --> T1 --> T2 --> T3 --> T4
    T4 --> E1 & E2
    F1 --> V1 --> V2
    V2 & F2 --> V3
    E1 & E3 --> C1
    E2 --> C2
    V3 --> C3
    F2 -. online lookup .-> E1
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / PostgreSQL)]
        A2[S3 Data Lake]
        A3[MSK Kafka]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Airflow + Spark EMR\nBatch Compute]
        B2[Flink / Lambda\nStream Compute]
    end

    subgraph Feast["Feature Store — Feast"]
        C1[Offline Store\nS3 + Athena]
        C2[Online Store\nRedis ElastiCache]
        C3[Feature Registry\nPostgreSQL]
    end

    subgraph MLflow["MLflow + Airflow"]
        D1[Airflow DAG\nOrchestration]
        D2[Training Job\nGPU / EMR]
        D3[MLflow Registry\nModel Versions]
    end

    subgraph VectorRAG["Qdrant + LangChain"]
        E1[Embedding Pipeline\nHuggingFace]
        E2[Qdrant\nVector Search]
        E3[LangChain\nRAG Chain]
    end

    subgraph Serving
        F1[BentoML Endpoint\nReal-Time]
        F2[Batch Scores\nS3 Output]
    end

    A1 & A2 --> B1 --> C1
    A3 --> B2 --> C2

    C1 & C3 --> D1 --> D2 --> D3
    D3 --> F1 & F2

    C1 --> E1 --> E2
    E2 & C2 --> E3
```

---

## Storage Zone Design

```
s3://<company>-ml-oss/
│
├── raw-features/
│   └── {source}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet · raw signals
│
├── feast-offline/                          ← Feast Offline Store
│   └── {feature_view}/{entity}/
│       └── year=YYYY/month=MM/day=DD/
│           └── *.parquet
│
├── training-datasets/
│   └── {model-name}/{experiment-id}/
│       ├── train.parquet
│       ├── validation.parquet
│       └── test.parquet
│
├── mlflow-artifacts/                       ← MLflow artifact store
│   └── {experiment-id}/{run-id}/
│       └── artifacts/  (model + plots + metrics)
│
├── embeddings/
│   └── {collection}/{run-date}/
│       └── *.parquet (id + vector + metadata)
│
└── batch-predictions/
    └── {model-name}/{score-date}/
        └── predictions.parquet
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│           IAM + Feast ACL + MLflow Auth + Qdrant API Keys     │
│                                                              │
│  IAM Role / Group       Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer            Full               All S3 · MLflow · Feast │
│  data-scientist         Read + Write       Offline Store · MLflow Experiments │
│  ml-ops                 Deploy only        MLflow Registry · BentoML │
│  feature-consumer       Read only          Online Store (Redis) │
│  llm-app                API key            Qdrant · LangChain endpoint │
│  analyst                Read only          Batch predictions S3 │
│                                                              │
│  Redis AUTH             → ElastiCache in-transit TLS + auth  │
│  Qdrant API Keys        → per-collection access control      │
│  MLflow Auth            → Basic auth / LDAP on EC2 deploy    │
│  KMS                    → SSE on all S3 zones                │
│  VPC                    → Qdrant · Redis · MLflow in private subnet │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Daily Airflow DAG\nfeature_pipeline]
    T2[📡 Kafka Consumer\nstream feature trigger]

    T1 --> J1[Spark on EMR\nBatch Feature Compute]
    T2 --> J2[Lambda / Flink\nStream Feature Materialize]

    J1 --> J3[Feast Materialize\nOffline → S3]
    J2 --> J4[Feast Materialize Online\nOffline → Redis]

    J3 --> J5[Airflow DAG\ntraining_pipeline]
    J5 --> J6[EMR / GPU Training Job\nPyTorch / XGBoost]
    J6 --> J7[MLflow Log\nMetrics + Artifacts]
    J7 -->|metrics pass| J8[MLflow Register\nStaging]
    J8 -->|manual approval| J9[BentoML Deploy\nReal-Time Endpoint]
    J9 --> N1[Slack Alert\nDeploy complete]

    J6 -->|fail| A1[Airflow email/Slack\nAlert on failure]

    V1[⏰ Nightly Embedding DAG] --> V2[HuggingFace Batch\nGenerate Vectors]
    V2 --> V3[Qdrant Upsert\nRefresh Collection]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | Amazon S3 | All zones: features, training, artifacts, predictions |
| Batch Feature Compute | Apache Spark on Amazon EMR | PySpark; EMR Serverless for cost efficiency |
| Stream Feature Compute | AWS Lambda + MSK (Kafka) | Near-real-time materialization into Redis |
| Feature Store (Offline) | Feast + S3 + Athena | Feast registry in PostgreSQL on RDS |
| Feature Store (Online) | Feast + Redis ElastiCache | Sub-10ms feature lookup for inference |
| Training Orchestration | Apache Airflow (MWAA) | DAG per model; trigger on new data |
| Training Jobs | EMR Spark / EC2 GPU (p3/p4) | XGBoost, PyTorch, Scikit-learn |
| Experiment Tracking | MLflow Tracking Server | Hosted on EC2; artifacts in S3 |
| Model Registry | MLflow Model Registry | Staging → Production promotion workflow |
| Real-Time Serving | BentoML on EKS | Containerized; auto-scales on HPA |
| Batch Scoring | Spark on EMR | S3 in → S3 out |
| Embedding Pipeline | HuggingFace Transformers | GPU batch transform on EC2 |
| Vector Store | Qdrant on EKS | Persistent volume; HNSW index |
| RAG Orchestration | LangChain | Retrieval chain: Qdrant → LLM (OpenAI or Bedrock) |
| API Gateway | AWS NLB + API Gateway | Route real-time + RAG traffic |
| Monitoring | Prometheus + Grafana on EKS | Model latency, drift metrics |
| Alerts | Airflow email + Slack Webhook | Pipeline failure notifications |
| Encryption | AWS KMS + Redis TLS | At-rest and in-transit |

---

## Comparison vs 8.1 — AWS Fully Managed

| Dimension | 8.2 AWS OSS on Cloud | 8.1 AWS Fully Managed |
|-----------|---------------------|----------------------|
| Feature Store | Feast + Redis | SageMaker Feature Store |
| Training Orchestration | Airflow (MWAA) | SageMaker Pipelines |
| Experiment Tracking | MLflow | SageMaker Experiments |
| Model Registry | MLflow Registry | SageMaker Model Registry |
| Vector Store | Qdrant on EKS | Amazon OpenSearch |
| LLM / RAG | LangChain + OpenAI/Bedrock | Amazon Bedrock native |
| Serving | BentoML on EKS | SageMaker Endpoints |
| Ops Burden | Medium — manage OSS infra | Low — fully managed |
| Portability | High — runs anywhere | AWS lock-in |
| Cost Model | EC2 + EKS + EMR | SageMaker per-hour pricing |
| Best For | Multi-cloud ambition, OSS flexibility | AWS-first, fast time to value |
