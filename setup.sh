#!/bin/bash

# Путь к проекту (WSL путь)
PROJECT_DIR="/home/oleksandr/apps/coingecko"
cd $PROJECT_DIR

echo "--- Создание виртуального окружения в $PROJECT_DIR ---"
python3 -m venv coingecko_venv

# Активация окружения
source coingecko_venv/bin/activate

echo "--- Обновление pip ---"
pip install --upgrade pip

echo "--- Установка библиотек для Ingestion (dlt) ---"
pip install "dlt[postgres]" requests

echo "--- Установка dbt и адаптера для Postgres ---"
pip install dbt-core dbt-postgres

echo "--- Установка Airflow (локальная версия для обучения) ---"
pip install apache-airflow
pip install astronomer-cosmos

echo "--- Установка дополнительных утилит для dbt ---"
# Это нужно для работы расширенных тестов, которые мы обсуждали
pip install dbt-utils dbt-expectations

echo "--- Установка MLflow, xgboost и сопутствующих библиотек ---"
pip install mlflow pandas xgboost scikit-learn seaborn

echo "--- Проверка установки ---"
dbt --version
airflow version

echo "-------------------------------------------------------"
echo "Готово! Для активации окружения используй: source coingecko_venv/bin/activate"