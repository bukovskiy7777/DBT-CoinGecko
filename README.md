# 🚀 Crypto Analytics Pipeline (dlt + dbt + Airflow + Cosmos + Postgres + MLflow + XGBoost)

An end-to-end **ELT & MLOps pipeline** designed to ingest, transform, and analyze cryptocurrency market data, followed by automated daily model retraining. This project demonstrates production-grade data engineering practices, including incremental loading, data quality testing, workflow orchestration, and model performance tracking.

---

## 🏗 System Architecture

The pipeline follows the Modern Data Stack architecture:
1.  **Ingestion (Extract & Load):** A Python-based `dlt` (Data Load Tool) script fetches historical market data from the **CoinGecko API**.
2.  **Storage:** Raw data is landed in a **PostgreSQL** "Raw" schema.
3.  **Transformation (T):** **dbt Core** handles data modeling across multiple layers.
4.  **Orchestration:** **Apache Airflow** schedules and monitors the entire workflow.
5.  **Integration:** **Astronomer Cosmos** maps dbt models directly into the Airflow DAG for granular task monitoring.
6.  **MLOps:** Automated retraining of an **XGBoost** binary classifier to predict price movement direction, with metadata tracked in **MLflow**.

**Airflow DAG Structure**
<img width="1154" height="221" alt="data_pipeline-graph" src="https://github.com/user-attachments/assets/19ba39cb-04fe-4c20-b34c-fc3c9a525eec" />

---

## 🛠 Tech Stack

* **Language:** Python 3.10+
* **Orchestration:** Apache Airflow
* **Data Ingestion:** `dlt` (Data Load Tool)
* **Transformation:** `dbt-core` (Postgres adapter)
* **Database:** PostgreSQL
* **ML Stack:** XGBoost, Scikit-learn, MLflow
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
│   │   ├── load_coingecko.py  # dlt ingestion logic
│   │   └── train_model.py     # ML training & MLflow logging
│   └── data_pipeline.py       # Airflow DAG using Cosmos
├── coingecko_dbt/             # dbt Project
│   ├── models/
│   │   ├── staging/           # stg_prices.sql (Views)
│   │   └── marts/             # fct_prices.sql (Incremental) - Business logic & ML features
│   ├── packages.yml           # packages need to install
│   └── dbt_project.yml        # dbt configuration
├── coingecko_venv/            # Python virtual environment
├── params.yml                 # Static config (coins, table names)
├── setup.sh                   # Environment setup script
└── README.md

```

---

## 📊 Machine Learning & MLOps

After the dbt transformations are complete, the pipeline triggers an ML task that performs the following:

* **Model:** XGBoost Classifier (Binary Classification).
* **Target:** Predicts if the price will go UP (1) or DOWN (0) for the next period.
* **Tracking (MLflow):** * **Parameters:** `n_estimators`, `max_depth`, `learning_rate` (optimized via GridSearchCV).
    * **Metrics:** Accuracy, Precision, Recall, and F1-Score.
    * **Artifacts:** Model binary, Feature Importance plot, and Confusion Matrix.

### Model Analysis
| Feature Importance | Confusion Matrix |
| :---: | :---: |
| <img width="640" height="480" alt="feature_importance_price_direction" src="https://github.com/user-attachments/assets/9a811b4c-5908-4dbc-875b-7ce86952f849" /> | <img width="800" height="600" alt="confusion_matrix_price_direction" src="https://github.com/user-attachments/assets/1ab7e5a0-c74a-47c1-bad2-49a4162c2e72" /> |

---

## 🚀 Getting Started

### Prerequisites
* WSL2 with Ubuntu 22.04

* PostgreSQL installed and running

* CoinGecko Demo API Key

### Installation
1. Clone the repository:

```text
Bash
git clone https://github.com/bukovskiy7777/DBT-CoinGecko.git
cd DBT-CoinGecko
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

Set `coingecko_project_dir` Variable in Airflow UI.

5. Install DBT deps:
```text
Bash
dbt build
cd coingecko_dbt
dbt deps
```

6. Run MLflow:
```text
Bash
mlflow ui --port 5000
```

---

## 👤 Author

Oleksandr