---
layout: default
title: "8.1 — AI/ML Data Platform · AWS Fully Managed"
---

# 8.1 — AI/ML Data Platform · AWS Fully Managed

**Stack:** SageMaker Feature Store · SageMaker Pipelines · SageMaker Model Registry · OpenSearch (vector) · Amazon Bedrock · S3  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[RDS / Aurora\nTransactional]
        S2[S3 Data Lake\nRaw Features]
        S3[Kinesis Stream\nEvent Data]
        S4[SaaS / APIs\nExternal Signals]
    end

    subgraph INGEST["Ingestion & Feature Engineering"]
        I1[AWS Glue ETL\nBatch Feature Compute]
        I2[Kinesis Firehose\nStream → Feature]
        I3[SageMaker Processing\nFeature Transform Jobs]
    end

    subgraph FEATURE["Feature Store — SageMaker Feature Store"]
        F1[Offline Store\nS3 · Parquet · historical]
        F2[Online Store\nDynamoDB-backed · low-latency]
    end

    subgraph TRAIN["Training Pipeline — SageMaker Pipelines"]
        T1[Data Prep Step\nSageMaker Processing]
        T2[Training Step\nSageMaker Training Job]
        T3[Evaluation Step\nModel Metrics]
        T4[Model Registry\nversioned artifacts]
    end

    subgraph VECTOR["Vector & LLM Layer"]
        V1[Embedding Pipeline\nSageMaker Batch Transform]
        V2[OpenSearch\nVector Index · kNN]
        V3[Amazon Bedrock\nFoundation Models · RAG]
    end

    subgraph SERVE["Serving Layer"]
        E1[SageMaker Endpoint\nReal-Time Inference]
        E2[SageMaker Batch Transform\nBatch Scoring]
        E3[Lambda + API GW\nFeature + Inference API]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Decisions]
        C2[BI / Analytics\nBatch Predictions]
        C3[LLM Chatbot\nRAG Responses]
    end

    SRC --> INGEST
    I1 & I2 & I3 --> F1
    I2 --> F2
    F1 --> T1 --> T2 --> T3 --> T4
    T4 --> E1 & E2
    F1 --> V1 --> V2
    V2 & F2 --> V3
    E1 & E3 --> C1
    E2 --> C2
    V3 --> C3
    F2 -. online lookup .-> E3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(RDS / Aurora)]
        A2[S3 Data Lake]
        A3[Kinesis Stream]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Glue ETL\nBatch Compute]
        B2[SageMaker Processing\nTransform Jobs]
        B3[Kinesis Firehose\nStream Ingest]
    end

    subgraph FeatureStore["SageMaker Feature Store"]
        C1[Offline Store\nS3 · Parquet]
        C2[Online Store\nDynamoDB · ms latency]
    end

    subgraph TrainingPipeline["SageMaker Pipelines"]
        D1[Processing Job]
        D2[Training Job]
        D3[Model Registry]
    end

    subgraph VectorLayer["Vector / RAG"]
        E1[Batch Embeddings\nSageMaker Transform]
        E2[OpenSearch\nkNN Vector Index]
        E3[Bedrock\nFoundation Model]
    end

    subgraph Serving
        F1[SageMaker Endpoint\nReal-Time]
        F2[Batch Transform\nOffline Scoring]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C2

    C1 --> D1 --> D2 --> D3
    D3 --> F1 & F2

    C1 --> E1 --> E2
    E2 & C2 --> E3

    F1 & F2 -->|predictions| C1
```

---

## Storage Zone Design

```
s3://<company>-ml-platform/
│
├── raw-features/
│   └── {domain}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet · pre-transform signals
│
├── feature-store/                          ← SageMaker Feature Store offline
│   └── {account-id}/sagemaker/
│       └── {feature-group-name}/
│           └── year=YYYY/month=MM/day=DD/hour=HH/
│               └── *.parquet
│
├── training-datasets/
│   └── {experiment-id}/{run-id}/
│       ├── train/    · validation/    · test/
│       └── metadata.json
│
├── model-artifacts/
│   └── {model-name}/{version}/
│       └── model.tar.gz · metrics.json
│
├── embeddings/
│   └── {collection}/{batch-date}/
│       └── *.parquet (id + vector)
│
└── predictions/
    └── {model-name}/{run-date}/
        └── batch-output/
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│             AWS IAM + SageMaker Domain Access Control         │
│                                                              │
│  IAM Role / User        Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer            Full               All zones + jobs  │
│  data-scientist         Read + Execute     Feature Store · Training │
│  ml-ops                 Read + Deploy      Model Registry · Endpoints │
│  feature-consumer       Read only          Online Store API  │
│  llm-app                Invoke only        Bedrock · Endpoints │
│  bi-analyst             Read only          Predictions output │
│                                                              │
│  SageMaker Domain       → network isolation per team         │
│  VPC Endpoints          → private S3 / SageMaker access      │
│  KMS                    → SSE on all S3 + Feature Store      │
│  Model Card             → bias/fairness tracking per model   │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Daily Schedule\nEventBridge Cron]
    T2[📡 Event Trigger\nNew data in S3 landing]

    T1 --> J1[Glue ETL Job\nCompute Batch Features]
    T2 --> J1

    J1 --> J2[SageMaker Feature Group\nIngest Offline Store]
    J2 --> J3[SageMaker Pipeline\nData Prep → Train → Evaluate]
    J3 -->|pass| J4[Model Registry\nRegister · Pending Approval]
    J4 -->|approved| J5[SageMaker Endpoint Update\nBlue/Green Deploy]
    J5 --> N1[SNS · Slack\nDeploy notification]

    J3 -->|fail metrics| A1[CloudWatch Alarm\n→ SNS → PagerDuty]
    J5 -->|fail canary| A1

    V1[⏰ Embedding Refresh\nnightly] --> V2[SageMaker Batch Transform\nGenerate Embeddings]
    V2 --> V3[OpenSearch Bulk Index\nUpdate kNN Index]
```

---

## Component Map

| Component | AWS Service | Notes |
|-----------|-------------|-------|
| Object Storage | S3 | Feature Store offline, training data, model artifacts |
| Batch Feature Compute | AWS Glue ETL | PySpark jobs; scheduled via EventBridge |
| Stream Feature Compute | Kinesis Firehose + Lambda | Near-real-time feature materialization |
| Feature Store (Offline) | SageMaker Feature Store | S3-backed; Athena queryable |
| Feature Store (Online) | SageMaker Feature Store | DynamoDB-backed; <10ms p99 lookup |
| Training Pipeline | SageMaker Pipelines | DAG of Processing → Training → Evaluation steps |
| Training Jobs | SageMaker Training | Managed Spot instances; distributed XGBoost/PyTorch |
| Model Registry | SageMaker Model Registry | Version tracking; approval workflow |
| Real-Time Serving | SageMaker Endpoints | Auto-scaling; multi-model endpoints |
| Batch Scoring | SageMaker Batch Transform | S3 in → S3 out; large-scale scoring |
| Embedding Generation | SageMaker Batch Transform | Text → vectors using hosted embedding model |
| Vector Store | Amazon OpenSearch | kNN plugin; HNSW index |
| Foundation Models / RAG | Amazon Bedrock | Claude / Titan / Llama; RAG via OpenSearch retrieval |
| API Layer | AWS Lambda + API Gateway | Online feature + inference routing |
| Orchestration | SageMaker Pipelines + EventBridge | Native ML DAG; cron scheduling |
| Monitoring | SageMaker Model Monitor | Data drift + prediction quality checks |
| Experiment Tracking | SageMaker Experiments | Linked to Pipelines runs |
| Secrets / Keys | AWS KMS + Secrets Manager | Encryption + credential management |
| Audit | CloudTrail + CloudWatch | API audit + metric dashboards |

---

## Comparison vs 8.2 — AWS OSS

| Dimension | 8.1 AWS Fully Managed | 8.2 AWS OSS on Cloud |
|-----------|----------------------|----------------------|
| Feature Store | SageMaker Feature Store | Feast on RDS/Redis |
| Training Orchestration | SageMaker Pipelines | Apache Airflow (MWAA) |
| Experiment Tracking | SageMaker Experiments | MLflow on EC2/ECS |
| Model Registry | SageMaker Model Registry | MLflow Model Registry |
| Vector Store | Amazon OpenSearch | Qdrant / Weaviate on EC2 |
| LLM / RAG | Amazon Bedrock | LangChain + self-hosted or OpenAI API |
| Serving | SageMaker Endpoints | BentoML / Triton on EC2/EKS |
| Ops Burden | Low — fully managed | Medium — manage OSS infra |
| Cost Model | Pay per use (SageMaker pricing) | EC2 + managed overhead |
| Portability | AWS lock-in | OSS portable |
| Best For | AWS-committed teams, speed to value | Cost-sensitive, multi-cloud ambition |
