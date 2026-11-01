"""A simple sales workflow."""
from datetime import datetime, timezone
from sales_logic import calculate_revenue

from airflow.sdk import dag, task
from datetime import datetime, timezone, timedelta
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv

@dag(
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["learning", "sales"],
)
def hello_sales():
    @task
    def extract():
        with open(
            "/opt/airflow/data/sales_2026-10-30.csv",
            newline="",
            encoding="utf-8-sig",
        ) as file:
            reader = csv.DictReader(file)

            required = {
                "order_id",
                "order_date",
                "product",
                "quantity",
                "unit_price_cents",
            }

            missing = required - set(reader.fieldnames or [])

            if missing:
                raise AirflowFailException(
                    f"Missing columns: {', '.join(sorted(missing))}"
                )

            orders = list(reader)

        for row in orders:
            row["quantity"] = int(row["quantity"])
            row["unit_price_cents"] = int(row["unit_price_cents"])

        return orders
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def transform(orders):
        try:
            return calculate_revenue(orders)
        except ValueError as error:
            raise AirflowFailException(str(error)) from error
    @task
    def report(total_cents):
        print(f"Total sales: ${total_cents / 100:.2f}")
    @task
    def load_orders(orders):
        hook = PostgresHook(postgres_conn_id="sales_postgres")

        sql = """
            INSERT INTO sales_orders (
                order_id, order_date, product,
                quantity, unit_price_cents
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                order_date = EXCLUDED.order_date,
                product = EXCLUDED.product,
                quantity = EXCLUDED.quantity,
                unit_price_cents = EXCLUDED.unit_price_cents;
        """

        with hook.get_conn() as connection:
            with connection.cursor() as cursor:
                for row in orders:
                    cursor.execute(
                        sql,
                        (
                            int(row["order_id"]),
                            row["order_date"],
                            row["product"],
                            row["quantity"],
                            row["unit_price_cents"],
                        ),
                    )

        return len(orders)
    @task
    def summarize_sales():
        hook = PostgresHook(postgres_conn_id="sales_postgres")

        rows = hook.get_records("""
            SELECT
                order_date,
                product,
                SUM(quantity) AS units_sold,
                SUM(quantity * unit_price_cents) / 100.0
                    AS revenue_dollars
            FROM sales_orders
            GROUP BY order_date, product
            ORDER BY order_date, product;
        """)

        for order_date, product, units, revenue in rows:
            print(
                f"Date: {order_date}, Product: {product}, "
                f"Units: {units}, Revenue: ${revenue:.2f}"
            )
    @task
    def check_revenue(expected_cents):
        hook = PostgresHook(postgres_conn_id="sales_postgres")

        result = hook.get_first("""
            SELECT COALESCE(
                SUM(quantity * unit_price_cents), 0
            )
            FROM sales_orders;
        """)

        actual_cents = result[0]

        if actual_cents != expected_cents:
            raise AirflowFailException(
                f"Revenue mismatch: expected {expected_cents} cents, "
                f"found {actual_cents} cents"
            )

        print(f"Revenue check passed: {actual_cents} cents")

    orders = extract()
    total = transform(orders)
    loaded = load_orders(orders)
    checked = check_revenue(total)
    summary = summarize_sales()

    total >> loaded
    loaded >> checked
    checked >> summary
    summary >> report(total)


hello_sales()