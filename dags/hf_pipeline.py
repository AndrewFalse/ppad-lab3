import json
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, Param, TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import hf_api


def get_orgs(params):
    # для проверок список организаций можно передать в conf запуска
    if params["orgs"]:
        return params["orgs"]
    hook = PostgresHook(postgres_conn_id="dwh")
    rows = hook.get_records("select org from stg.hf_orgs order by org")
    return [row[0] for row in rows]


def snapshot_date(dag_run):
    # у ручного запуска в Airflow 3 нет logical_date, поэтому дату снимка берём из run_after
    return dag_run.run_after.date().isoformat()


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

    @task
    def load_license_tags(dag_run=None, run_id=None):
        day = snapshot_date(dag_run)
        data = hf_api.get_license_tags()

        hook = PostgresHook(postgres_conn_id="dwh")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("delete from stg.hf_license_tags where business_date = %s", (day,))
        cur.execute(
            "insert into stg.hf_license_tags (business_date, payload, load_id) values (%s, %s::jsonb, %s)",
            (day, json.dumps(data), run_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        print("license tags", len(data["license"]))

    @task
    def load_models(dag_run=None, run_id=None, params=None):
        day = snapshot_date(dag_run)
        orgs = get_orgs(params)

        hook = PostgresHook(postgres_conn_id="dwh")
        conn = hook.get_conn()
        cur = conn.cursor()
        total = 0
        for org in orgs:
            models, pages = hf_api.list_models(org)
            # снимок организации за день перезаписываем целиком в одной транзакции
            cur.execute("delete from stg.hf_models where business_date = %s and org = %s", (day, org))
            cur.execute("delete from stg.hf_list_runs where business_date = %s and org = %s", (day, org))
            for model in models:
                cur.execute(
                    "insert into stg.hf_models (business_date, org, payload, load_id) values (%s, %s, %s::jsonb, %s)",
                    (day, org, json.dumps(model), run_id),
                )
            cur.execute(
                "insert into stg.hf_list_runs (business_date, org, pages, models, load_id) values (%s, %s, %s, %s, %s)",
                (day, org, pages, len(models), run_id),
            )
            conn.commit()
            print(org, "models", len(models), "pages", pages)
            total += len(models)
        cur.close()
        conn.close()
        return total

    with TaskGroup("extract"):
        create_stg_tables = SQLExecuteQueryOperator(
            task_id="create_stg_tables",
            conn_id="dwh",
            sql=["stg/01_create_tables.sql", "stg/02_orgs.sql"],
        )
        create_stg_tables >> load_license_tags() >> load_models()


hf_pipeline()
