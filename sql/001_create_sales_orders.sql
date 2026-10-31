CREATE TABLE IF NOT EXISTS sales_orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL,
    product TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0)
);