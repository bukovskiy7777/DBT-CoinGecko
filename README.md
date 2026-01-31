# 🚀 Crypto Analytics Pipeline (dlt + dbt + Airflow + Postgres)

An end-to-end **ELT pipeline** designed to ingest, transform, and analyze cryptocurrency market data. This project demonstrates production-grade data engineering practices, including incremental loading, data quality testing, and workflow orchestration.

---

## 🏗 System Architecture

The pipeline follows the Modern Data Stack architecture:
1.  **Ingestion (Extract & Load):** A Python-based `dlt` (Data Load Tool) script fetches historical market data from the **CoinGecko API**.
2.  **Storage:** Raw data is landed in a **PostgreSQL** "Raw" schema.
3.  **Transformation (T):** **dbt Core** handles data modeling across multiple layers.
4.  **Orchestration:** **Apache Airflow** schedules and monitors the entire workflow.
5.  **Integration:** **Astronomer Cosmos** maps dbt models directly into the Airflow DAG for granular task monitoring.

---

## 🛠 Tech Stack

* **Language:** Python 3.10+
* **Orchestration:** Apache Airflow
* **Data Ingestion:** `dlt` (Data Load Tool)
* **Transformation:** `dbt-core` (Postgres adapter)
* **Database:** PostgreSQL
* **DAG Visualization:** Astronomer Cosmos
* **Environment:** WSL2 (Ubuntu 22.04)

---

## 📈 Key Engineering Features

### 1. Idempotency & Backfilling
The pipeline is designed to be **idempotent**. By using Airflow's logical date (`ds`), the ingestion script fetches data for specific historical dates. This allows for safe **backfilling** (using `catchup=True`) without duplicating data or creating gaps.

### 2. Multi-Layer dbt Modeling
* **Staging Layer:** Views that clean, cast, and rename raw JSON fields.
* **Marts Layer:** Incremental tables calculating business logic, such as **7-day Moving Averages** using SQL window functions.

### 3. Incremental Loading with Lookback
To optimize performance, the pipeline only processes "new" data daily. For analytical metrics like Moving Averages, a **lookback window** logic is implemented to ensure the model has access to the previous 7 days of history during the incremental run.

### 4. Data Quality Framework
Data integrity is enforced via **dbt tests**:
* `not_null` and `unique` constraints on critical keys.
* `accepted_range` tests for financial metrics (price, volume).
* Automatic DAG failure in Airflow if data quality thresholds are not met.

---

## 📂 Project Structure

```text
.
├── dags/
│   ├── scripts/
│   │   └── load_coingecko.py  # dlt ingestion script
│   └── data_pipeline.py       # Airflow DAG using Cosmos
├── coingecko_dbt/             # dbt Project
│   ├── models/
│   │   ├── staging/           # stg_prices.sql (Views)
│   │   └── marts/             # fct_prices.sql (Incremental)
│   ├── packages.yml           # packages need to install
│   └── dbt_project.yml
├── coingecko_venv/            # Python virtual environment
├── params.yml                 # DAG ingestion params
├── setup.sh                   # Environment setup script
└── README.md

```

---

## 🚀 Getting Started

### Prerequisites
WSL2 with Ubuntu 22.04

PostgreSQL installed and running

CoinGecko Demo API Key

### Installation
1. Clone the repository:

```text
Bash
git clone [https://github.com/yourusername/crypto-analytics-pipeline.git](https://github.com/yourusername/crypto-analytics-pipeline.git)
cd crypto-analytics-pipeline
```

2. Run the setup script to create a virtual environment and install dependencies:

```text
Bash
chmod +x setup.sh
./setup.sh
```

3. Configure Airflow Connections:

Create a Connection ID `postgres_ubuntu` in the Airflow UI with your PostgreSQL credentials.

4. Start Airflow:

```text
Bash
airflow standalone
```

5. Install DBT deps:
```text
Bash
cd coingecko_dbt
dbt deps
```

---

## 👤 Author

Oleksandr