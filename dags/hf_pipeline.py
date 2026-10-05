from datetime import timedelta

import pendulum
from airflow.sdk import dag, Param, TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="hf_pipeline",
    # расписание включим, когда будет готов весь конвейер
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    template_searchpath=["/opt/airflow/sql"],
    # для проверок можно передать список организаций: {"orgs": ["yandex"]}
    params={"orgs": Param([], type="array", items={"type": "string"})},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["hf"],
)
def hf_pipeline():

    with TaskGroup("extract"):
        create_stg_tables = SQLExecuteQueryOperator(
            task_id="create_stg_tables",
            conn_id="dwh",
            sql=["stg/01_create_tables.sql", "stg/02_orgs.sql"],
        )


hf_pipeline()
