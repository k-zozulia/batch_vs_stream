"""
Generator for dimension tables: customers and products.
Creates synthetic reference data for enriching orders.
"""

import csv
import random
import yaml
from pathlib import Path
from datetime import datetime, timedelta


def load_config():
    """Loads configuration from YAML file."""
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def generate_customers(num_customers):
    """Generate synthetic customer dimension table."""

    first_names = [
        "John",
        "Jane",
        "Michael",
        "Emily",
        "David",
        "Sarah",
        "Robert",
        "Lisa",
        "James",
        "Mary",
        "William",
        "Patricia",
        "Richard",
        "Jennifer",
        "Thomas",
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
        "Martinez",
        "Hernandez",
        "Lopez",
        "Wilson",
        "Anderson",
    ]

    segments = ["Premium", "Standard", "Basic", "VIP"]

    customers = []

    print(f"Generating {num_customers} customers...")

    for i in range(1, num_customers + 1):
        customer_id = f"c{i}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        customer_name = f"{first_name} {last_name}"
        customer_email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"
        customer_segment = random.choice(segments)

        days_ago = random.randint(1, 730)
        registration_date = (datetime.now() - timedelta(days=days_ago)).strftime(
            "%Y-%m-%d"
        )

        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "customer_email": customer_email,
                "customer_segment": customer_segment,
                "registration_date": registration_date,
            }
        )

    print(f"Generated {len(customers)} customers")
    return customers


def generate_products(num_products):
    """Generate synthetic product dimension table."""

    categories = [
        "Electronics",
        "Clothing",
        "Home & Kitchen",
        "Sports",
        "Books",
        "Toys",
        "Beauty",
        "Automotive",
        "Garden",
        "Health",
    ]

    brands = [
        "BrandA",
        "BrandB",
        "BrandC",
        "BrandD",
        "BrandE",
        "BrandF",
        "BrandG",
        "BrandH",
        "BrandI",
        "BrandJ",
    ]

    product_types = {
        "Electronics": ["Laptop", "Phone", "Tablet", "Headphones", "Camera"],
        "Clothing": ["Shirt", "Jeans", "Dress", "Jacket", "Shoes"],
        "Home & Kitchen": ["Blender", "Coffee Maker", "Pan", "Knife Set", "Toaster"],
        "Sports": [
            "Running Shoes",
            "Yoga Mat",
            "Dumbbells",
            "Tennis Racket",
            "Backpack",
        ],
        "Books": [
            "Fiction Novel",
            "Cookbook",
            "Self-Help Book",
            "Biography",
            "Textbook",
        ],
        "Toys": ["Action Figure", "Board Game", "Puzzle", "Doll", "Building Blocks"],
        "Beauty": ["Shampoo", "Lipstick", "Face Cream", "Perfume", "Nail Polish"],
        "Automotive": [
            "Car Charger",
            "Air Freshener",
            "Phone Mount",
            "Dash Cam",
            "Seat Cover",
        ],
        "Garden": ["Seeds", "Planter", "Garden Tools", "Hose", "Fertilizer"],
        "Health": [
            "Vitamins",
            "First Aid Kit",
            "Thermometer",
            "Protein Powder",
            "Yoga Block",
        ],
    }

    products = []

    print(f"Generating {num_products} products...")

    for i in range(1, num_products + 1):
        product_id = f"p{i}"
        category = random.choice(categories)
        product_type = random.choice(product_types[category])
        brand = random.choice(brands)
        product_name = f"{brand} {product_type}"
        list_price = round(random.uniform(10.0, 600.0), 2)

        products.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "brand": brand,
                "list_price": list_price,
            }
        )

    print(f"Generated {len(products)} products")
    return products


def save_customers_to_csv(customers, output_file):
    """Save customers to CSV file."""

    output_path = Path(__file__).parent.parent.parent / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "customer_id",
        "customer_name",
        "customer_email",
        "customer_segment",
        "registration_date",
    ]

    print("Saving customers to CSV...")

    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)

    print(f"Customers saved to {output_path}")


def save_products_to_csv(products, output_file):
    """Save products to CSV file."""

    output_path = Path(__file__).parent.parent.parent / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["product_id", "product_name", "category", "brand", "list_price"]

    print("Saving products to CSV...")

    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(products)

    print(f"Products saved to {output_path}")


def main():
    """Main function for generating dimension tables."""

    config = load_config()

    num_customers = config["generator"]["num_customers"]
    num_products = config["generator"]["num_products"]

    customers = generate_customers(num_customers)
    save_customers_to_csv(customers, "data/dimensions/customers.csv")

    products = generate_products(num_products)
    save_products_to_csv(products, "data/dimensions/products.csv")

    print("\n✓ Dimension tables generated successfully!")


if __name__ == "__main__":
    main()
