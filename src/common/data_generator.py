"""
Synthetic e-commerce orders data generator.
Generates CSV with intentional data quality issues for testing ETL pipelines.
"""

import csv
import random
import yaml
from datetime import datetime, timedelta
from pathlib import Path


def load_config():
    """Loads configuration from YAML file."""

    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def generate_timestamp(start_date, end_date):
    """Generates random timestamp between start_date and end_date."""

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)


def generate_orders(config):
    """Generates orders according to config."""

    gen_config = config["generator"]

    num_records = gen_config["num_records"]
    start_date = gen_config["start_date"]
    end_date = gen_config["end_date"]

    num_products = gen_config["num_products"]
    num_customers = gen_config["num_customers"]

    dup_rate = gen_config["duplicate_rate"]
    neg_qty_rate = gen_config["negative_quantity_rate"]
    neg_price_rate = gen_config["negative_price_rate"]
    late_rate = gen_config["late_data_rate"]
    missing_rate = gen_config["missing_id_rate"]

    statuses = [
        "completed",
        "Completed",
        "COMPLETED",
        "Complete",
        "cancelled",
        "Cancelled",
        "CANCELLED",
        "pending",
        "Pending",
        "PENDING",
        "processing",
        "Done",
        "done",
    ]

    orders = []
    timestamps = []

    print("Generating orders...")

    for i in range(num_records):
        order_id = f"o{i+1}"
        customer_id = f"c{random.randint(1, num_customers)}"
        product_id = f"p{random.randint(1, num_products)}"

        order_timestamp = generate_timestamp(start_date, end_date)
        quantity = random.randint(1, 10)
        price = round(random.uniform(5.0, 500.0), 2)
        status = random.choice(statuses)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "order_timestamp": order_timestamp,
                "quantity": quantity,
                "price": price,
                "status": status,
            }
        )

    orders.sort(key=lambda x: x["order_timestamp"])

    issues_count = {
        "duplicates": 0,
        "negative_quantity": 0,
        "negative_price": 0,
        "late_data": 0,
        "missing_order_id": 0,
    }

    num_duplicates = int(num_records * dup_rate)
    duplicate_indices = random.sample(range(len(orders)), num_duplicates)

    for index in duplicate_indices:
        duplicate = orders[index].copy()
        duplicate["order_timestamp"] = duplicate["order_timestamp"] + timedelta(
            seconds=random.randint(10, 300)
        )
        orders.append(duplicate)
        issues_count["duplicates"] += 1

    num_neg_qty = int(num_records * neg_qty_rate)
    negative_qty_indices = random.sample(range(len(orders)), num_neg_qty)

    for index in negative_qty_indices:
        orders[index]["quantity"] = -abs(orders[index]["quantity"])
        issues_count["negative_quantity"] += 1

    num_neg_price = int(num_records * neg_price_rate)
    negative_price_indices = random.sample(range(len(orders)), num_neg_price)

    for index in negative_price_indices:
        orders[index]["price"] = -abs(orders[index]["price"])
        issues_count["negative_price"] += 1

    num_late_data = int(num_records * late_rate)
    late_data = random.sample(range(len(orders)), num_late_data)

    for index in late_data:
        delay_minutes = random.randint(1, 60)
        orders[index]["order_timestamp"] = orders[index]["order_timestamp"] - timedelta(
            minutes=delay_minutes
        )
        issues_count["late_data"] += 1

    num_missing_id = int(num_records * missing_rate)
    missing_id_indices = random.sample(
        range(len(orders)), min(num_missing_id, len(orders))
    )

    for index in missing_id_indices:
        orders[index]["order_id"] = None
        issues_count["missing_order_id"] += 1

    orders.sort(key=lambda x: x["order_timestamp"])

    print(f"Generated {len(orders)} orders")
    print(f"Duplicates: {issues_count['duplicates']}")
    print(f"Negative Qty: {issues_count['negative_quantity']}")
    print(f"Negative Price: {issues_count['negative_price']}")
    print(f"Late Data: {issues_count['late_data']}")

    return orders


def save_to_csv(orders, output_file):
    """Saves orders to a CSV file."""

    output_path = Path(__file__).parent.parent.parent / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "order_id",
        "customer_id",
        "product_id",
        "order_timestamp",
        "quantity",
        "price",
        "status",
    ]

    print("Saving orders to CSV...")

    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for order in orders:
            writer.writerow(
                {
                    "order_id": order["order_id"],
                    "customer_id": order["customer_id"],
                    "product_id": order["product_id"],
                    "order_timestamp": order["order_timestamp"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "quantity": order["quantity"],
                    "price": order["price"],
                    "status": order["status"],
                }
            )

    print("Saved orders to CSV")


def main():
    """Main function for generating data."""

    config = load_config()
    orders = generate_orders(config)

    output_file = config["generator"]["output_file"]
    save_to_csv(orders, output_file)

    save_to_csv(orders, output_file)


if __name__ == "__main__":
    main()
