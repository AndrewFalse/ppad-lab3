import json
import os
import re
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, Param, TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import hf_api


SQL_DIR = "/opt/airflow/sql"


def sql_files(folder):
    # файлы слоя выполняются по порядку имён: 01_..., 02_...
    path = os.path.join(SQL_DIR, folder)
    if not os.path.isdir(path):
        return []
    names = []
    for name in sorted(os.listdir(path)):
        if name.endswith(".sql"):
            names.append(name)
    return names


def chain_sql_files(folder, check=False):
    # задачи слоя идут друг за другом, имя задачи это имя файла без .sql
    previous = EmptyOperator(task_id="start")
    for name in sql_files(folder):
        if check:
            step = SQLCheckOperator(task_id=name[:-4], conn_id="dwh", sql=folder + "/" + name, retries=0)
        else:
            step = SQLExecuteQueryOperator(task_id=name[:-4], conn_id="dwh", sql=folder + "/" + name)
        previous >> step
        previous = step


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


def check_fresh_run(dag_run):
    # снимок показывает текущее состояние, поэтому перезапуск старого запуска записал бы его под старой датой
    if dag_run.run_after.date() != pendulum.now("UTC").date():
        raise ValueError("this run is not from today, trigger a new run instead of clearing an old one")


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
        check_fresh_run(dag_run)
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
        check_fresh_run(dag_run)
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
        # коммиты берём только у открытых моделей вне группы quant, у которых появился новый sha
        cur.execute(
            """
            select m.payload->>'_id', m.payload->>'id'
            from stg.hf_models m
            where m.business_date = %s
              and m.org = any(%s)
              and m.payload->>'gated' = 'false'
              and m.org not in (select org from stg.hf_orgs where stratum = 'quant')
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
            for i, commit in enumerate(commits):
                # API отдаёт коммиты от новых к старым, поэтому самый старый получает номер 1
                number = len(commits) - i
                cur.execute(
                    "insert into stg.hf_commits (hf_id, repo_id, sha, commit_number, payload, load_id) "
                    "values (%s, %s, %s, %s, %s::jsonb, %s) on conflict (hf_id, sha) do nothing",
                    (hf_id, repo_id, commit["id"], number, json.dumps(commit), run_id),
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
                    # в карточке бывает что угодно, берём только имена вида name или org/name
                    if isinstance(item, str) and re.match(r"^[\w.-]+(/[\w.-]+)?$", item):
                        bases.add(item)

        # модели из снимка дня и уже загруженные базовые модели повторно не берём
        cur.execute("select org from stg.hf_orgs")
        scope = set(row[0] for row in cur.fetchall())
        cur.execute("select org from stg.hf_list_runs where business_date = %s", (day,))
        snapshot_orgs = set(row[0] for row in cur.fetchall())
        cur.execute("select payload->>'id' from stg.hf_models where business_date = %s", (day,))
        in_snapshot = set(row[0] for row in cur.fetchall())
        cur.execute("select repo_id from stg.hf_base_models")
        loaded = set(row[0] for row in cur.fetchall())
        todo = []
        for base in sorted(bases - in_snapshot - loaded):
            org = base.split("/")[0]
            # модель из охвата придёт со снимком своей организации, если его сегодня ещё нет
            # если снимок есть, а модели в нём нет, её переименовали или удалили, такую загружаем
            if org in scope and org not in snapshot_orgs:
                continue
            todo.append(base)
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

    # проверку не повторяем: если данные плохие, повтор ничего не исправит
    @task(retries=0)
    def check_stg(dag_run=None, params=None):
        day = snapshot_date(dag_run)
        orgs = get_orgs(params)
        hook = PostgresHook(postgres_conn_id="dwh")
        errors = []

        # по каждой организации есть непустой снимок за день
        rows = hook.get_records(
            "select org, models from stg.hf_list_runs where business_date = %s",
            parameters=(day,),
        )
        loaded = {}
        for row in rows:
            loaded[row[0]] = row[1]
        for org in orgs:
            if loaded.get(org, 0) == 0:
                errors.append("no snapshot for " + org)

        # число моделей в снимке совпадает с журналом загрузки
        rows = hook.get_records(
            """
            select r.org
            from stg.hf_list_runs r
            left join stg.hf_models m on m.business_date = r.business_date and m.org = r.org
            where r.business_date = %s
            group by r.org, r.models
            having r.models <> count(m.payload)
            """,
            parameters=(day,),
        )
        for row in rows:
            errors.append("snapshot size mismatch for " + row[0])

        # одна модель встречается в снимке один раз
        rows = hook.get_records(
            "select count(*) from (select payload->>'_id' from stg.hf_models "
            "where business_date = %s group by 1 having count(*) > 1) t",
            parameters=(day,),
        )
        if rows[0][0] > 0:
            errors.append("duplicate models in snapshot")

        # у каждого коммита есть дата
        rows = hook.get_records("select count(*) from stg.hf_commits where payload->>'date' is null")
        if rows[0][0] > 0:
            errors.append("commits without date")

        # у каждой ревизии есть свой коммит
        rows = hook.get_records(
            "select count(*) from stg.hf_revisions r where not exists "
            "(select 1 from stg.hf_commits c where c.hf_id = r.hf_id and c.sha = r.sha)"
        )
        if rows[0][0] > 0:
            errors.append("revisions without commit")

        # payload заполнен ровно у ответов с кодом 200
        rows = hook.get_records(
            "select (select count(*) from stg.hf_revisions where (http_status = 200) <> (payload is not null)) "
            "+ (select count(*) from stg.hf_base_models where (http_status = 200) <> (payload is not null))"
        )
        if rows[0][0] > 0:
            errors.append("payload does not match http status")

        rows = hook.get_records(
            """
            select
                (select count(*) from stg.hf_models where business_date = %s),
                (select count(*) from stg.hf_commits),
                (select count(*) from stg.hf_revisions),
                (select count(*) from stg.hf_revisions where http_status <> 200),
                (select count(*) from stg.hf_base_models)
            """,
            parameters=(day,),
        )
        stats = rows[0]
        print("models", stats[0], "commits", stats[1], "revisions", stats[2],
              "revisions not 200", stats[3], "base models", stats[4])

        # эти счётчики не роняют проверку, но показывают, всё ли догрузилось
        rows = hook.get_records(
            """
            select
                (select count(*) from stg.hf_models m
                 where m.business_date = %s
                   and m.org = any(%s)
                   and m.payload->>'gated' = 'false'
                   and m.org not in (select org from stg.hf_orgs where stratum = 'quant')
                   and not exists (select 1 from stg.hf_commits c
                                   where c.hf_id = m.payload->>'_id' and c.sha = m.payload->>'sha')),
                (select count(*) from stg.hf_commits c
                 where not exists (select 1 from stg.hf_revisions r
                                   where r.hf_id = c.hf_id and r.sha = c.sha)),
                (select count(*) from stg.hf_base_models where http_status <> 200)
            """,
            parameters=(day, orgs),
        )
        late = rows[0]
        print("open models without head commit", late[0], "commits without revision", late[1],
              "base models not 200", late[2])

        if errors:
            raise ValueError("; ".join(errors))

    with TaskGroup("extract") as extract_group:
        create_stg_tables = SQLExecuteQueryOperator(
            task_id="create_stg_tables",
            conn_id="dwh",
            sql=["stg/01_create_tables.sql", "stg/02_orgs.sql"],
        )
        create_stg_tables >> load_license_tags() >> load_models() >> load_commits() >> load_revisions() >> load_base_models() >> check_stg()

    # роль 2 кладёт sql/dds/*.sql и проверки sql/dq/*.sql, роль 3 кладёт sql/cdm/*.sql
    with TaskGroup("dds") as dds_group:
        chain_sql_files("dds")

    # проверка возвращает одну строку, все значения в ней должны быть true
    with TaskGroup("dq") as dq_group:
        chain_sql_files("dq", check=True)

    with TaskGroup("cdm") as cdm_group:
        chain_sql_files("cdm")

    extract_group >> dds_group >> dq_group >> cdm_group


hf_pipeline()
