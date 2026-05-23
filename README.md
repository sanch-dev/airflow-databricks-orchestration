# Airflow Databricks Orchestration

## 🎯 Overview

Apache Airflow orchestration layer for the **News Intelligence Pipeline** running on Databricks. This project demonstrates production-grade data pipeline orchestration using external orchestration (Airflow) to control transformations on Databricks.

---

## 🏗️ Architecture

┌─────────────────────────────────────────────────────────────┐
│            Apache Airflow (Docker on Azure VM)              │
│                   Orchestration Layer                        │
└─────────────────────────────────────────────────────────────┘
│
┌─────────────────────┼─────────────────────┐
↓                     ↓                     ↓
┌──────────┐         ┌────────────┐        ┌──────────┐
│  Bronze  │         │   Silver   │        │   Gold   │
│ Ingestion│────────▶│ Transform  │───────▶│ Embedding│
│ (NewsAPI)│         │   (DLT)    │        │(OpenAI)  │
└──────────┘         └────────────┘        └──────────┘
│                     │                     │
└─────────────────────┼─────────────────────┘
↓
Databricks Cluster
(Delta Lake, Unity Catalog)


---

## 🛠️ Tech Stack

- **Orchestration**: Apache Airflow 2.10.0
- **Containerization**: Docker + Docker Compose
- **Data Warehouse**: Databricks (Delta Lake, Unity Catalog)
- **Compute**: Azure VM (B2as_v2, Ubuntu 24.04)
- **Database**: PostgreSQL 15
- **Message Queue**: Kafka 7.5.0 + Zookeeper
- **APIs**: NewsAPI, Azure OpenAI
- **Language**: Python 3.11

---

## 📊 Pipeline Tasks

1. **bronze_ingestion** - Fetch news from NewsAPI, store raw JSON
2. **silver_transformation** - DLT pipeline for cleaning & transformations
3. **gold_embedding** - Generate embeddings with Azure OpenAI
4. **log_pipeline_failure** - Error logging (runs if silver fails)

**Dependencies**: bronze → silver → gold, and silver → log_failure

**Performance**: ~2.5 min end-to-end, 1000+ articles/day, scheduled daily at 00:00 UTC

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Databricks workspace with Unity Catalog
- Azure OpenAI API key
- NewsAPI key

### Setup

1. **Clone repository**
```bash
git clone https://github.com/sanch-dev/airflow-databricks-orchestration.git
cd airflow-databricks-orchestration
```

2. **Configure environment**
```bash
nano .env
```

Add these variables:
```env
DATABRICKS_HOST=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
OPENAI_API_KEY=sk-...
NEWS_API_KEY=...
AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://:token@host
```

3. **Deploy**
```bash
docker-compose up -d
```

4. **Access Airflow**
- URL: http://localhost:8080
- Login: admin / admin

5. **Trigger pipeline**
- Runs automatically daily at 00:00 UTC
- Or manually click play button in UI

---

## 📁 Project Structure

.
├── dags/
│   └── news_intelligence_dag.py    # Main Airflow DAG
├── docker-compose.yml               # All services
├── Dockerfile                       # Airflow image
├── requirements.txt                 # Python dependencies
├── .env                             # Credentials (git-ignored)
└── README.md                        # This file



---

## ⚙️ Configuration

### Databricks Connection

Uses environment variable-based authentication:

```python
DatabricksSubmitRunOperator(
    task_id='bronze_ingestion',
    databricks_conn_id='databricks_default',
    existing_cluster_id='0502-130412-dpe6g4gr',
    notebook_task={
        'notebook_path': '/Workspace/news_pipeline/bronze/01_auto_loader_ingest',
    },
)
```

### Kafka Integration

- **Producer**: Sends news to `news` topic
- **Consumer**: Databricks reads from Kafka
- **Broker**: localhost:9092

---

## ✨ Key Features

✅ Containerized with Docker for easy deployment  
✅ Modular DAG with task separation and error handling  
✅ Real-time streaming via Kafka  
✅ Runs on scalable Databricks clusters  
✅ Full Airflow logging and monitoring  
✅ Idempotent tasks with retries  
✅ Secure credential management via .env  

---

## 🔗 Related Projects

**[databricks-news-pipeline](https://github.com/sanch-dev/databricks-news-pipeline)** - Databricks-native orchestration for comparison

---

## 📧 Contact

- GitHub: [sanch-dev](https://github.com/sanch-dev)
- LinkedIn: https://www.linkedin.com/in/sanchit-dass-18945315a/
- Email: sdass979665@gmail.com
---

## 📝 License

MIT License - Open Source
