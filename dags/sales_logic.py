def calculate_revenue(orders):
    for row in orders:
        if row["quantity"] <= 0:
            raise ValueError(
                f"Invalid quantity for {row['product']}"
            )

        if row["unit_price_cents"] < 0:
            raise ValueError(
                f"Invalid price for {row['product']}"
            )

    return sum(
        row["quantity"] * row["unit_price_cents"]
        for row in orders
    )