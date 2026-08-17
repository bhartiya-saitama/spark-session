"""Generate the synthetic retail e-commerce dataset for the Spark hands-on session.

Standard library only. Deterministic (seeded), so every attendee sees the same
numbers and the talking points stay accurate.

    python3 data_generator/generate_data.py

Writes gzipped files to ./data/. The messiness in the output is deliberate:
each defect is the setup for a specific teaching moment in the notebook.
"""

import csv
import gzip
import json
import os
import random
from datetime import datetime, timedelta

SEED = 42
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

N_CUSTOMERS = 5_000
N_PRODUCTS = 500
N_ORDERS = 60_000
N_DUPLICATE_ORDERS = 500
N_ORDER_ITEMS = 150_000
N_EVENTS = 80_000

START = datetime(2024, 1, 1)
END = datetime(2025, 12, 31)

COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AE", "AU", "BR", "JP", "FR"]
COUNTRY_WEIGHTS = [30, 22, 10, 8, 7, 6, 5, 5, 4, 3]

CITIES = {
    "IN": ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune"],
    "US": ["New York", "Chicago", "Austin", "Seattle", "Boston"],
    "GB": ["London", "Manchester", "Bristol"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "SG": ["Singapore"],
    "AE": ["Dubai", "Abu Dhabi"],
    "AU": ["Sydney", "Melbourne"],
    "BR": ["Sao Paulo", "Rio de Janeiro"],
    "JP": ["Tokyo", "Osaka"],
    "FR": ["Paris", "Lyon"],
}

FIRST_NAMES = [
    "Aarav", "Diya", "Rohan", "Ananya", "Vikram", "Meera", "Arjun", "Kavya",
    "James", "Emma", "Liam", "Olivia", "Noah", "Sophia", "Lucas", "Mia",
    "Hans", "Greta", "Yuki", "Haruto", "Chen", "Wei", "Omar", "Layla",
    "Pedro", "Camila", "Jack", "Chloe", "Nina", "Tomas",
]
LAST_NAMES = [
    "Sharma", "Patel", "Reddy", "Iyer", "Nair", "Gupta", "Smith", "Johnson",
    "Brown", "Wilson", "Muller", "Schmidt", "Tanaka", "Sato", "Silva",
    "Santos", "Chen", "Wang", "Khan", "Ali", "Dubois", "Martin",
]

SEGMENTS = ["Consumer", "SMB", "Enterprise"]
SEGMENT_WEIGHTS = [70, 22, 8]

# Deliberately uneven so the groupBy result is interesting and the skew
# talking point has something real to point at.
CATEGORIES = ["Electronics", "Apparel", "Home", "Grocery", "Beauty", "Sports", "Toys"]
CATEGORY_WEIGHTS = [38, 20, 14, 10, 8, 6, 4]

PRODUCT_NOUNS = {
    "Electronics": ["Headphones", "Laptop", "Monitor", "Keyboard", "Smartwatch", "Speaker", "Router"],
    "Apparel": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Scarf", "Hoodie"],
    "Home": ["Lamp", "Cushion", "Cookware Set", "Curtains", "Storage Box"],
    "Grocery": ["Coffee Beans", "Olive Oil", "Granola", "Green Tea", "Pasta"],
    "Beauty": ["Face Serum", "Shampoo", "Lip Balm", "Sunscreen"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Running Belt", "Water Bottle"],
    "Toys": ["Puzzle", "Building Blocks", "Board Game"],
}
PRODUCT_ADJS = ["Classic", "Pro", "Lite", "Max", "Eco", "Urban", "Prime", "Nova", "Core"]

ORDER_STATUS = ["COMPLETED", "PENDING", "CANCELLED", "RETURNED"]
ORDER_STATUS_WEIGHTS = [78, 10, 7, 5]
CHANNELS = ["web", "mobile_app", "store", "partner"]
CHANNEL_WEIGHTS = [45, 35, 15, 5]

EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout", "login"]
EVENT_TYPE_WEIGHTS = [55, 20, 13, 7, 5]
OS_LIST = ["Android", "iOS", "Windows", "macOS", "Linux"]
OS_WEIGHTS = [40, 30, 18, 10, 2]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [60, 33, 7]
PAGES = ["/home", "/search", "/product", "/cart", "/checkout", "/account", "/deals"]


def random_ts(rnd):
    delta = END - START
    return START + timedelta(seconds=rnd.randint(0, int(delta.total_seconds())))


def fmt_ts(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def fmt_ts_eu(ts):
    """Day-first format. Spark's CSV inference cannot type this, and a naive
    to_timestamp() silently returns nulls -- which is the whole point."""
    return ts.strftime("%d-%m-%Y %H:%M:%S")


def write_csv_gz(path, header, rows):
    with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def gen_customers(rnd):
    """3% null city, 1% null country, mixed-case country codes."""
    rows = []
    stats = {"null_city": 0, "null_country": 0, "mixed_case_country": 0}
    for i in range(1, N_CUSTOMERS + 1):
        customer_id = f"C{i:06d}"
        first = rnd.choice(FIRST_NAMES)
        last = rnd.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{rnd.randint(1, 999)}@example.com"
        country = rnd.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        city = rnd.choice(CITIES[country])

        if rnd.random() < 0.03:
            city = ""
            stats["null_city"] += 1

        country_out = country
        if rnd.random() < 0.01:
            country_out = ""
            stats["null_country"] += 1
        elif rnd.random() < 0.12:
            country_out = rnd.choice([country.lower(), country.capitalize()])
            stats["mixed_case_country"] += 1

        signup = (START - timedelta(days=rnd.randint(0, 900))).strftime("%Y-%m-%d")
        segment = rnd.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        rows.append([customer_id, name, email, city, country_out, signup, segment])

    header = ["customer_id", "name", "email", "city", "country", "signup_date", "segment"]
    return header, rows, stats


def gen_products(rnd):
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        product_id = f"P{i:05d}"
        category = rnd.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        name = f"{rnd.choice(PRODUCT_ADJS)} {rnd.choice(PRODUCT_NOUNS[category])}"
        base = {
            "Electronics": (49, 1499),
            "Apparel": (12, 180),
            "Home": (15, 320),
            "Grocery": (3, 45),
            "Beauty": (6, 90),
            "Sports": (10, 250),
            "Toys": (8, 120),
        }[category]
        unit_price = round(rnd.uniform(*base), 2)
        supplier_country = rnd.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        rows.append([product_id, name, category, unit_price, supplier_country])

    header = ["product_id", "product_name", "category", "unit_price", "supplier_country"]
    return header, rows


def gen_orders(rnd, customer_ids):
    """order_ts as a day-first string; 500 exact duplicate rows mixed in."""
    rows = []
    for i in range(1, N_ORDERS + 1):
        order_id = f"O{i:07d}"
        customer_id = rnd.choice(customer_ids)
        order_ts = fmt_ts_eu(random_ts(rnd))
        status = rnd.choices(ORDER_STATUS, weights=ORDER_STATUS_WEIGHTS, k=1)[0]
        channel = rnd.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        rows.append([order_id, customer_id, order_ts, status, channel, "USD"])

    duplicates = [list(r) for r in rnd.sample(rows, N_DUPLICATE_ORDERS)]
    rows.extend(duplicates)
    rnd.shuffle(rows)

    header = ["order_id", "customer_id", "order_ts", "status", "channel", "currency"]
    stats = {"duplicate_rows": N_DUPLICATE_ORDERS}
    return header, rows, stats


def gen_order_items(rnd, order_ids, products):
    """Orphan product_ids, negative quantities, 'N/A' unit_price strings.

    'N/A' rather than an empty field on purpose: Spark reads an empty CSV field
    as null and would still infer DoubleType, so the inference gotcha would not
    fire. A non-numeric token forces the column to StringType.
    """
    price_by_product = {p[0]: p[3] for p in products}
    product_ids = list(price_by_product.keys())
    orphan_ids = [f"P9{i:04d}" for i in range(1, 41)]

    rows = []
    stats = {"orphan_product_rows": 0, "negative_quantity": 0, "na_unit_price": 0}
    for i in range(1, N_ORDER_ITEMS + 1):
        order_item_id = f"OI{i:08d}"
        order_id = rnd.choice(order_ids)

        if rnd.random() < 0.0013:
            product_id = rnd.choice(orphan_ids)
            unit_price = round(rnd.uniform(10, 300), 2)
            stats["orphan_product_rows"] += 1
        else:
            product_id = rnd.choice(product_ids)
            unit_price = price_by_product[product_id]

        quantity = rnd.choices([1, 2, 3, 4, 5], weights=[50, 25, 13, 8, 4], k=1)[0]
        if rnd.random() < 0.01:
            quantity = -quantity
            stats["negative_quantity"] += 1

        unit_price_out = unit_price
        if rnd.random() < 0.02:
            unit_price_out = "N/A"
            stats["na_unit_price"] += 1

        discount_pct = round(rnd.choices([0.0, 0.05, 0.1, 0.15, 0.2, 0.3],
                                         weights=[55, 15, 12, 8, 7, 3], k=1)[0], 2)
        rows.append([order_item_id, order_id, product_id, quantity, unit_price_out, discount_pct])

    header = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"]
    return header, rows, stats


def gen_events(rnd, customer_ids, path):
    """JSON Lines with a nested `device` object, written streaming to keep memory flat."""
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for i in range(1, N_EVENTS + 1):
            record = {
                "event_id": f"E{i:08d}",
                "ts": fmt_ts(random_ts(rnd)),
                "customer_id": rnd.choice(customer_ids),
                "event_type": rnd.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0],
                "device": {
                    "os": rnd.choices(OS_LIST, weights=OS_WEIGHTS, k=1)[0],
                    "type": rnd.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0],
                },
                "page": rnd.choice(PAGES),
            }
            fh.write(json.dumps(record) + "\n")


def human_size(path):
    size = os.path.getsize(path)
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def main():
    rnd = random.Random(SEED)
    os.makedirs(OUT_DIR, exist_ok=True)

    cust_header, cust_rows, cust_stats = gen_customers(rnd)
    prod_header, prod_rows = gen_products(rnd)
    customer_ids = [r[0] for r in cust_rows]

    order_header, order_rows, order_stats = gen_orders(rnd, customer_ids)
    order_ids = [r[0] for r in order_rows]

    item_header, item_rows, item_stats = gen_order_items(rnd, order_ids, prod_rows)

    files = {
        "customers.csv.gz": (cust_header, cust_rows),
        "products.csv.gz": (prod_header, prod_rows),
        "orders.csv.gz": (order_header, order_rows),
        "order_items.csv.gz": (item_header, item_rows),
    }
    for filename, (header, rows) in files.items():
        write_csv_gz(os.path.join(OUT_DIR, filename), header, rows)

    events_path = os.path.join(OUT_DIR, "events.jsonl.gz")
    gen_events(rnd, customer_ids, events_path)

    print(f"Wrote to {OUT_DIR}\n")
    print(f"{'file':<22}{'rows':>10}{'size':>12}")
    print("-" * 44)
    for filename, (_, rows) in files.items():
        path = os.path.join(OUT_DIR, filename)
        print(f"{filename:<22}{len(rows):>10,}{human_size(path):>12}")
    print(f"{'events.jsonl.gz':<22}{N_EVENTS:>10,}{human_size(events_path):>12}")

    print("\nSeeded defects (these drive the teaching moments):")
    all_stats = {**cust_stats, **order_stats, **item_stats}
    for key, value in all_stats.items():
        print(f"  {key:<24}{value:>8,}")

    assert cust_stats["null_city"] > 0
    assert cust_stats["mixed_case_country"] > 0
    assert item_stats["orphan_product_rows"] > 0
    assert item_stats["negative_quantity"] > 0
    assert item_stats["na_unit_price"] > 0


if __name__ == "__main__":
    main()
