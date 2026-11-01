import pytest
from dags.sales_logic import calculate_revenue


def test_calculate_revenue():
    orders = [
        {"product": "Notebook", "quantity": 4, "unit_price_cents": 500},
        {"product": "Pen", "quantity": 3, "unit_price_cents": 200},
    ]

    result = calculate_revenue(orders)

    assert result == 2600
def test_negative_quantity_is_rejected():
    orders = [
        {"product": "Notebook", "quantity": -2, "unit_price_cents": 500},
    ]

    with pytest.raises(ValueError, match="Invalid quantity"):
        calculate_revenue(orders)


def test_negative_price_is_rejected():
    orders = [
        {"product": "Pen", "quantity": 3, "unit_price_cents": -200},
    ]

    with pytest.raises(ValueError, match="Invalid price"):
        calculate_revenue(orders)