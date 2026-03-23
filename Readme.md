# 🚀 Crypto ETL Pipeline using Apache Airflow

## 📌 Overview

This project implements an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline fetches real-time cryptocurrency prices, processes the data, and stores it in a structured format.

## ⚙️ Tech Stack

* Apache Airflow
* Docker & Docker Compose
* Python
* REST API (CoinGecko)

## 🔄 Pipeline Workflow

1. **Extract**: Fetch cryptocurrency prices from CoinGecko API
2. **Transform**:

   * Data cleaning
   * Data type conversion
   * Timestamp feature engineering
3. **Load**: Store processed data into CSV file

## 📁 Project Structure

```
crypto-airflow-etl-pipeline/
├── dags/
├── data/
├── logs/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## ▶️ How to Run

### 1. Clone Repository

```
git clone https://github.com/your-username/crypto-airflow-etl-pipeline.git
cd crypto-airflow-etl-pipeline
```

### 2. Start Airflow

```
docker-compose up
```

### 3. Access Airflow UI

http://localhost:8080

### 4. Enable DAG

* Turn ON `crypto_etl_pipeline`
* Monitor execution

## 📊 Output

Processed data is stored in:

```
data/output.csv
```

## 🧠 Key Learnings

* Building ETL pipelines with Airflow
* Task dependency management
* XCom usage for data passing
* Docker-based orchestration

## 🚀 Future Improvements

* Integrate Kafka for real-time streaming
* Store data in cloud (AWS S3 / GCP)
* Add data validation and alerting

---

**Author:** Syed Huzaifa Ali
