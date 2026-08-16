"""
DAG principal del pipeline medallion: genera datos nuevos, corre dbt
(bronze -> silver -> gold), corre tests, y actualiza el snapshot de
clientes. Pensado para correr diariamente, simulando un pipeline real
de produccion.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "david",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="Pipeline medallion: ingesta -> dbt -> tests -> snapshot",
    schedule_interval="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["medallion", "dbt", "snowflake"],
) as dag:

    generar_datos = BashOperator(
        task_id="generar_datos",
        bash_command=f"cd {DBT_PROJECT_DIR} && python scripts/generate_dirty_data_snowflake.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot",
    )

    generar_datos >> dbt_run >> dbt_test >> dbt_snapshot
