import json
import re
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

    @task(execution_timeout=timedelta(hours=2))
    def load_commits(dag_run=None, run_id=None, params=None):
        day = snapshot_date(dag_run)
        orgs = get_orgs(params)

        hook = PostgresHook(postgres_conn_id="dwh")
        conn = hook.get_conn()
        cur = conn.cursor()
        # коммиты берём только у открытых моделей, у которых появился новый sha
        cur.execute(
            """
            select m.payload->>'_id', m.payload->>'id'
            from stg.hf_models m
            where m.business_date = %s
              and m.org = any(%s)
              and m.payload->>'gated' = 'false'
              and not exists (
                  select 1 from stg.hf_commits c
                  where c.hf_id = m.payload->>'_id' and c.sha = m.payload->>'sha'
              )
            order by 2
            """,
            (day, orgs),
        )
        todo = cur.fetchall()
        print("models to load commits", len(todo))

        added = 0
        for row in todo:
            hf_id = row[0]
            repo_id = row[1]
            status, commits = hf_api.list_commits(repo_id)
            if status != 200:
                print("skip", repo_id, status)
                continue
            for commit in commits:
                cur.execute(
                    "insert into stg.hf_commits (hf_id, repo_id, sha, payload, load_id) "
                    "values (%s, %s, %s, %s::jsonb, %s) on conflict (hf_id, sha) do nothing",
                    (hf_id, repo_id, commit["id"], json.dumps(commit), run_id),
                )
                added += cur.rowcount
            conn.commit()

        cur.close()
        conn.close()
        print("new commits", added)
        return added

    @task(execution_timeout=timedelta(hours=8))
    def load_revisions(dag_run=None, run_id=None, params=None):
        day = snapshot_date(dag_run)
        orgs = get_orgs(params)

        hook = PostgresHook(postgres_conn_id="dwh")
        conn = hook.get_conn()
        cur = conn.cursor()
        # для каждого нового коммита берём состояние карточки и файлов на этот коммит
        cur.execute(
            """
            select c.hf_id, c.repo_id, c.sha
            from stg.hf_commits c
            where c.hf_id in (
                select payload->>'_id' from stg.hf_models
                where business_date = %s and org = any(%s)
            )
              and not exists (
                  select 1 from stg.hf_revisions r
                  where r.hf_id = c.hf_id and r.sha = c.sha
              )
            order by c.repo_id, c.sha
            """,
            (day, orgs),
        )
        todo = cur.fetchall()
        print("revisions to load", len(todo))

        done = 0
        not_ok = 0
        for row in todo:
            hf_id = row[0]
            repo_id = row[1]
            sha = row[2]
            status, data = hf_api.get_revision(repo_id, sha)
            payload = None
            if data is not None:
                payload = json.dumps(data)
            else:
                # такой ответ сохраняется навсегда, поэтому пишем его в лог
                print("revision status", repo_id, sha, status)
                not_ok += 1
            cur.execute(
                "insert into stg.hf_revisions (hf_id, repo_id, sha, http_status, payload, load_id) "
                "values (%s, %s, %s, %s, %s::jsonb, %s) on conflict (hf_id, sha) do nothing",
                (hf_id, repo_id, sha, status, payload, run_id),
            )
            # сохраняем каждую ревизию сразу, чтобы после сбоя продолжить с того же места
            conn.commit()
            done += 1
            if done % 500 == 0:
                print("revisions loaded", done)

        cur.close()
        conn.close()
        print("revisions loaded", done, "not 200", not_ok)
        return done

    @task
    def load_base_models(dag_run=None, run_id=None, params=None):
        day = snapshot_date(dag_run)
        orgs = get_orgs(params)

        hook = PostgresHook(postgres_conn_id="dwh")
        conn = hook.get_conn()
        cur = conn.cursor()

        # base_model в карточке бывает строкой или списком
        cur.execute(
            "select payload->'cardData'->'base_model' from stg.hf_models where business_date = %s and org = any(%s)",
            (day, orgs),
        )
        bases = set()
        for row in cur.fetchall():
            value = row[0]
            if isinstance(value, str):
                value = [value]
            if isinstance(value, list):
                for item in value:
                    # в карточке бывает что угодно, берём только имена вида org/name
                    if isinstance(item, str) and re.match(r"^[\w.-]+(/[\w.-]+)?$", item):
                        bases.add(item)

        # модели из снимка дня и уже загруженные базовые модели повторно не берём
        cur.execute("select payload->>'id' from stg.hf_models where business_date = %s", (day,))
        in_snapshot = set(row[0] for row in cur.fetchall())
        cur.execute("select repo_id from stg.hf_base_models")
        loaded = set(row[0] for row in cur.fetchall())
        todo = sorted(bases - in_snapshot - loaded)
        print("base models to load", len(todo))

        for repo_id in todo:
            status, data = hf_api.get_model(repo_id)
            payload = None
            if data is not None:
                payload = json.dumps(data)
            cur.execute(
                "insert into stg.hf_base_models (repo_id, http_status, payload, load_id) "
                "values (%s, %s, %s::jsonb, %s) on conflict (repo_id) do nothing",
                (repo_id, status, payload, run_id),
            )
            conn.commit()

        cur.close()
        conn.close()
        return len(todo)

    with TaskGroup("extract"):
        create_stg_tables = SQLExecuteQueryOperator(
            task_id="create_stg_tables",
            conn_id="dwh",
            sql=["stg/01_create_tables.sql", "stg/02_orgs.sql"],
        )
        create_stg_tables >> load_license_tags() >> load_models() >> load_commits() >> load_revisions() >> load_base_models()


hf_pipeline()
