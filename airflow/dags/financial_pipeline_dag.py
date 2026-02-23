from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/pipeline"
VENV_PYTHON = "python3"
DBT_DIR = f"{PROJECT_DIR}/financial_pipeline_dbt"

default_args = {
    "owner": "swapnil",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="financial_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 23),
    schedule="0 9 * * 1-5",
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/src/data_ingestion.py",
    )

    process = BashOperator(
        task_id="process",
        bash_command=f"export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-25.jdk/Contents/Home && {VENV_PYTHON} {PROJECT_DIR}/src/data_processing.py",
    )

    load = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/src/bigquery_loader.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {PROJECT_DIR}/venv/bin/dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {PROJECT_DIR}/venv/bin/dbt test",
    )

    ingest >> process >> load >> dbt_run >> dbt_test
