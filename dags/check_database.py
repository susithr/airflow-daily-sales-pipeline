from datetime import datetime, timezone

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
)
def check_database():
    @task
    def check_connection():
        hook = PostgresHook(postgres_conn_id="sales_postgres")

        result = hook.get_first(
            "SELECT current_database(), current_user;"
        )

        print(f"Connected to database: {result[0]}")
        print(f"Connected as user: {result[1]}")

    check_connection()


check_database()