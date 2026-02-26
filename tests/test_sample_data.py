"""Tests for sample data generation."""

import csv
import json
from pathlib import Path

from data.sample.generate_sample_data import (
    generate_customers,
    generate_orders,
    generate_products,
    save_data,
)


class TestGenerateCustomers:
    """Tests for customer data generation."""

    def test_correct_count(self) -> None:
        """Generates requested number of customers."""
        customers = generate_customers(50)
        assert len(customers) == 50

    def test_unique_ids(self) -> None:
        """All customer IDs are unique."""
        customers = generate_customers(100)
        ids = [c["customer_id"] for c in customers]
        assert len(ids) == len(set(ids))

    def test_required_fields(self) -> None:
        """Each customer has required fields."""
        customers = generate_customers(10)
        required = {
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "created_at",
            "segment",
            "country",
        }
        for c in customers:
            assert required.issubset(c.keys())

    def test_id_format(self) -> None:
        """Customer IDs follow CUST format."""
        customers = generate_customers(5)
        for c in customers:
            assert c["customer_id"].startswith("CUST")

    def test_valid_segments(self) -> None:
        """Segments are from expected values."""
        customers = generate_customers(100)
        valid = {"retail", "wholesale", "enterprise"}
        for c in customers:
            assert c["segment"] in valid


class TestGenerateProducts:
    """Tests for product data generation."""

    def test_correct_count(self) -> None:
        """Generates requested number of products."""
        products = generate_products(25)
        assert len(products) == 25

    def test_unique_ids(self) -> None:
        """All product IDs are unique."""
        products = generate_products(100)
        ids = [p["product_id"] for p in products]
        assert len(ids) == len(set(ids))

    def test_required_fields(self) -> None:
        """Each product has required fields."""
        products = generate_products(10)
        required = {
            "product_id",
            "name",
            "category",
            "price",
            "cost",
            "stock_quantity",
            "supplier_id",
        }
        for p in products:
            assert required.issubset(p.keys())

    def test_positive_prices(self) -> None:
        """Prices are positive."""
        products = generate_products(50)
        for p in products:
            assert p["price"] > 0
            assert p["cost"] > 0


class TestGenerateOrders:
    """Tests for order data generation."""

    def test_correct_count(self) -> None:
        """Generates requested number of orders."""
        customers = generate_customers(10)
        products = generate_products(10)
        orders = generate_orders(customers, products, 50)
        assert len(orders) == 50

    def test_unique_ids(self) -> None:
        """All order IDs are unique."""
        customers = generate_customers(10)
        products = generate_products(10)
        orders = generate_orders(customers, products, 100)
        ids = [o["order_id"] for o in orders]
        assert len(ids) == len(set(ids))

    def test_valid_customer_references(self) -> None:
        """Order customer_ids reference valid customers."""
        customers = generate_customers(10)
        products = generate_products(10)
        orders = generate_orders(customers, products, 50)
        valid_ids = {c["customer_id"] for c in customers}
        for o in orders:
            assert o["customer_id"] in valid_ids

    def test_has_items(self) -> None:
        """Each order has at least one item."""
        customers = generate_customers(10)
        products = generate_products(10)
        orders = generate_orders(customers, products, 50)
        for o in orders:
            assert len(o["items"]) >= 1

    def test_total_amount_positive(self) -> None:
        """Order totals are positive."""
        customers = generate_customers(10)
        products = generate_products(10)
        orders = generate_orders(customers, products, 50)
        for o in orders:
            assert o["total_amount"] > 0

    def test_valid_statuses(self) -> None:
        """Order statuses are from expected values."""
        customers = generate_customers(10)
        products = generate_products(10)
        orders = generate_orders(customers, products, 100)
        valid = {"completed", "pending", "shipped", "cancelled", "returned"}
        for o in orders:
            assert o["status"] in valid


class TestSaveData:
    """Tests for data saving in different formats."""

    def test_save_json(self, tmp_path: Path) -> None:
        """Saves valid JSON file."""
        data = [{"id": 1, "name": "test"}]
        path = tmp_path / "test.json"
        save_data(data, path, "json")
        with open(path) as f:
            loaded = json.load(f)
        assert loaded == data

    def test_save_csv(self, tmp_path: Path) -> None:
        """Saves valid CSV file."""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        path = tmp_path / "test.csv"
        save_data(data, path, "csv")
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"

    def test_save_jsonl(self, tmp_path: Path) -> None:
        """Saves valid JSONL file."""
        data = [{"id": 1}, {"id": 2}, {"id": 3}]
        path = tmp_path / "test.jsonl"
        save_data(data, path, "jsonl")
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 3
        assert json.loads(lines[0])["id"] == 1

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Creates parent directories if needed."""
        path = tmp_path / "nested" / "dir" / "test.json"
        save_data([{"id": 1}], path, "json")
        assert path.exists()

    def test_save_empty_csv(self, tmp_path: Path) -> None:
        """Empty data produces no CSV file content."""
        path = tmp_path / "empty.csv"
        save_data([], path, "csv")
        # File should not be created for empty data
        assert not path.exists()
