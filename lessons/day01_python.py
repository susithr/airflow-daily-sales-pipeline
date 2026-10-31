"""Run with Python before introducing Airflow scheduling."""


def extract():
    return [
        {"product": "Notebook", "quantity": 2, "unit_price_cents": 500},
        {"product": "Pen", "quantity": 3, "unit_price_cents": 200},
    ]


def transform(orders):
    return sum(row["quantity"] * row["unit_price_cents"] for row in orders)


def report(total_cents):
    print(f"Total sales: ${total_cents / 100:.2f}")


if __name__ == "__main__":
    report(transform(extract()))
