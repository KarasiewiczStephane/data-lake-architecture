"""Synthetic e-commerce data generator for testing the data lake pipeline.

Generates customers, products, and orders datasets in JSON, CSV, and JSONL
formats for use with the bronze ingestion layer.
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_customers(n: int = 1000) -> list[dict]:
    """Generate synthetic customer records.

    Args:
        n: Number of customer records to generate.

    Returns:
        List of customer dictionaries.
    """
    first_names = [
        "John",
        "Jane",
        "Bob",
        "Alice",
        "Charlie",
        "Diana",
        "Eve",
        "Frank",
        "Grace",
        "Henry",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Wilson",
    ]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]
    segments = ["retail", "wholesale", "enterprise"]
    countries = ["US", "UK", "CA", "DE", "FR", "AU"]

    customers = []
    for i in range(1, n + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        customers.append(
            {
                "customer_id": f"CUST{i:06d}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{i}@{random.choice(domains)}",
                "created_at": (
                    datetime.now() - timedelta(days=random.randint(1, 730))
                ).isoformat(),
                "segment": random.choice(segments),
                "country": random.choice(countries),
            }
        )
    return customers


def generate_products(n: int = 500) -> list[dict]:
    """Generate synthetic product records.

    Args:
        n: Number of product records to generate.

    Returns:
        List of product dictionaries.
    """
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Toys"]
    adjectives = ["Premium", "Basic", "Pro", "Ultra", "Classic", "Modern"]
    nouns = ["Widget", "Gadget", "Device", "Tool", "Item", "Product"]

    products = []
    for i in range(1, n + 1):
        products.append(
            {
                "product_id": f"PROD{i:06d}",
                "name": f"{random.choice(adjectives)} {random.choice(nouns)} {i}",
                "category": random.choice(categories),
                "price": round(random.uniform(9.99, 999.99), 2),
                "cost": round(random.uniform(5.00, 500.00), 2),
                "stock_quantity": random.randint(0, 1000),
                "supplier_id": f"SUP{random.randint(1, 50):03d}",
            }
        )
    return products


def generate_orders(
    customers: list[dict],
    products: list[dict],
    n: int = 10000,
) -> list[dict]:
    """Generate synthetic order records with line items.

    Args:
        customers: List of customer dicts (need customer_id, country).
        products: List of product dicts (need product_id, price).
        n: Number of order records to generate.

    Returns:
        List of order dictionaries.
    """
    statuses = ["completed", "pending", "shipped", "cancelled", "returned"]

    orders = []
    for i in range(1, n + 1):
        customer = random.choice(customers)
        num_items = random.randint(1, 5)
        order_products = random.sample(products, min(num_items, len(products)))

        items = []
        total = 0.0
        for p in order_products:
            qty = random.randint(1, 3)
            items.append(
                {
                    "product_id": p["product_id"],
                    "quantity": qty,
                    "unit_price": p["price"],
                }
            )
            total += qty * p["price"]

        orders.append(
            {
                "order_id": f"ORD{i:08d}",
                "customer_id": customer["customer_id"],
                "order_date": (
                    datetime.now() - timedelta(days=random.randint(1, 365))
                ).isoformat(),
                "status": random.choice(statuses),
                "total_amount": round(total, 2),
                "items": items,
                "shipping_country": customer["country"],
            }
        )
    return orders


def save_data(data: list[dict], filepath: Path, fmt: str = "json") -> None:
    """Save data to file in the specified format.

    Args:
        data: List of records to save.
        filepath: Output file path.
        fmt: Output format (json, csv, jsonl).
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "json":
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
    elif fmt == "csv":
        if data:
            with open(filepath, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
    elif fmt == "jsonl":
        with open(filepath, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")


def main() -> None:
    """Generate all sample datasets and save to data/sample/."""
    output_dir = Path(__file__).parent

    random.seed(42)

    print("Generating customers...")
    customers = generate_customers(1000)
    save_data(customers, output_dir / "customers.json")
    save_data(customers, output_dir / "customers.csv", "csv")

    print("Generating products...")
    products = generate_products(500)
    save_data(products, output_dir / "products.json")

    print("Generating orders...")
    orders = generate_orders(customers, products, 10000)
    save_data(orders, output_dir / "orders.jsonl", "jsonl")

    print(f"Sample data generated in {output_dir}")
    print(f"  - customers: {len(customers)} records (JSON + CSV)")
    print(f"  - products: {len(products)} records (JSON)")
    print(f"  - orders: {len(orders)} records (JSONL)")


if __name__ == "__main__":
    main()
