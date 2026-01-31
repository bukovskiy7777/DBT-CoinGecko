from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook # NavigationBaseHook обычно используется в специфичных сборках, BaseHook универсален
from datetime import datetime
from airflow.models import Variable
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.macros import ds_add
import yaml

# Пути
PROJECT_DIR = Variable.get("coingecko_project_dir", default_var="/home/oleksandr/apps/coingecko")
DBT_PROJECT_PATH = f"{PROJECT_DIR}/coingecko_dbt"

# Настройка профиля dbt для Cosmos (чтобы не читать из ~/.dbt/profiles.yml)
profile_config = ProfileConfig(
    profile_name="coingecko_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_ubuntu", # Создай это подключение в UI Airflow
        profile_args={"schema": "dev_coingecko"},
    )
)

def run_ingestion_task(**context):
    from scripts.load_coingecko import run_load_to_postgres
    # Вычисляем дату за "вчера" относительно логической даты запуска
    target_date = ds_add(context['ds'], -1) 

    # Получаем API ключ из Connection
    api_conn = BaseHook.get_connection("coingecko_api")
    api_key = api_conn.password

    # Загружаем статические параметры из YAML
    with open(f"{PROJECT_DIR}/params.yml", 'r') as f:
        config = yaml.safe_load(f)
        
    coins = config['coingecko']['coins']
    dataset_name = config['coingecko']['dlt']['dataset_name']
    table_name = config['coingecko']['dlt']['table_name']
    
    # 2. Получаем БД креды из Connection
    db_conn = BaseHook.get_connection("postgres_ubuntu")
    creds = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/{db_conn.schema}"
    run_load_to_postgres(target_date, creds, api_key, coins, dataset_name, table_name)


with DAG(
    dag_id='data_pipeline',
    description='ETL pipeline for Coingecko data using dlt and dbt',
    schedule="30 8 * * *",  # Ежедневно в 08:30 утра
    start_date=datetime(2025, 1, 31),
    max_active_runs=1,  # Airflow будет запускать только ОДИН день за раз
    catchup=True, # Оставляем True, чтобы dlt/dbt прогнали историю за пропущенные дни
    tags=["dbt", "coingecko", "prices"],
    default_args={
        "retries": 1,
        "owner": "Analytics Team",
    },
) as dag:

    # 1. Загрузка
    ingest_data = PythonOperator(
        task_id='extract_and_load_data',
        python_callable=run_ingestion_task
    )

    # 2. dbt через Cosmos
    # Это создаст группу задач: для каждой модели dbt свой task в Airflow
    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        group_id="dbt_transformation",
        operator_args={
            "install_deps": True,
            "vars": {
                # ds — это дата логического запуска (logical date).
                # macros.ds_add(ds, -1) вычтет один день от ds.
                "execution_date": "{{ macros.ds_add(ds, -1) }}"
            }
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{PROJECT_DIR}/coingecko_venv/bin/dbt"
        ),
        render_config=RenderConfig(
            # Исключаем все модели из папки example
            exclude=["path:models/example"]
        ), 
    )

    ingest_data >> dbt_tg