# ⚽ Football Live Streaming Data Pipeline

A real-time football event streaming pipeline showcasing modern data engineering, stream processing, and Medallion (Lakehouse) architecture.

FastAPI → Kafka (3 brokers) → NiFi → PostgreSQL (Master & Replica) → Airflow & Spark ELT (Bronze–Silver–Gold)

## 📊 Architecture & Data Flow

![Data Workflow Diagram](docs/data_workflow_diagram.png) 

- **FastAPI** generates simulated live football match events
- **Kafka** provides high-throughput, fault-tolerant event streaming
- **Apache NiFi** consumes Kafka topics and persists raw events
- **PostgreSQL** stores event logs and curated analytics tables
- **Apache Spark (PySpark)** processes data using the Medallion architecture
- **Apache Airflow** orchestrates, schedules, and monitors Spark ETL jobs

---

## 🧱 Medallion Architecture

- **Bronze** – Immutable raw event ingestion from `event_logs`
- **Silver** – Cleaned, validated, and deduplicated events
- **Gold** – Business-ready aggregates (goals, fouls, shots per team)

---

## 🛠 Technology Stack

FastAPI · Kafka · Apache NiFi · PostgreSQL · Apache Spark · Apache Airflow · Python · SQL

---

## ⏱ ETL & Orchestration

- Streaming events first land in PostgreSQL event log tables
- PySpark ETL transforms data across **Bronze → Silver → Gold** layers
- Apache Airflow manages scheduling, retries, and observability
- JDBC-based reads and writes ensure reliable batch processing

---

## 🎯 Project Goal

A portfolio-grade data engineering project demonstrating:

- Real-time streaming architectures
- Scalable and reliable data ingestion
- Lakehouse / Medallion design patterns
- Spark + Airflow production workflows
- Sports analytics use cases

