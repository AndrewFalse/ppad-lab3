import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="check_dwh",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["service"],
)
def check_dwh():

    @task
    def check_schemas():
        hook = PostgresHook(postgres_conn_id="dwh")
        rows = hook.get_records(
            "select schema_name from information_schema.schemata "
            "where schema_name in ('stg', 'dds', 'cdm') order by schema_name"
        )
        names = [row[0] for row in rows]
        print("found schemas", names)
        if names != ["cdm", "dds", "stg"]:
            raise ValueError("not all schemas found in dwh")

    check_schemas()


check_dwh()
