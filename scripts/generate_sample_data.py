"""
Generate sample datasets for the NovaCart pipeline lab.
Creates both clean data and deliberately broken records to exercise
the quarantine and schema-drift paths.

Usage:
    python scripts/generate_sample_data.py
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
import csv

ROOT = Path(__file__).parent.parent
ORDERS_DIR   = ROOT / "data" / "landing" / "orders"
CUSTOMER_DIR = ROOT / "data" / "landing" / "customers"
DB_PATH      = ROOT / "data" / "landing" / "products.db"

ORDERS_DIR.mkdir(parents=True, exist_ok=True)
CUSTOMER_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# ── Orders CSVs (3 dates) ────────────────────────────────────────────────────

ORDERS = {
    "2025-11-07": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-001","CUST-001","PROD-001","2025-11-07","2","49.99","shipped"],
        ["ORD-002","CUST-002","PROD-002","2025-11-07","1","199.00","pending"],
        ["ORD-003","CUST-003","PROD-001","2025-11-07","5","49.99","delivered"],
        # Bad row — quantity = 0 → quarantine
        ["ORD-004","CUST-001","PROD-003","2025-11-07","0","29.99","pending"],
        # Duplicate of ORD-001 — deduplicated in Silver
        ["ORD-001","CUST-001","PROD-001","2025-11-07","2","49.99","shipped"],
    ],
    "2025-11-08": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-005","CUST-004","PROD-002","2025-11-08","3","199.00","shipped"],
        ["ORD-006","CUST-001","PROD-003","2025-11-08","1","29.99","delivered"],
        # Bad row — invalid status
        ["ORD-007","CUST-002","PROD-001","2025-11-08","2","49.99","refunded"],
    ],
    "2025-11-09": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-008","CUST-003","PROD-003","2025-11-09","4","29.99","pending"],
        ["ORD-009","CUST-005","PROD-001","2025-11-09","2","49.99","shipped"],
    ],
    "2025-11-10": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-010","CUST-002","PROD-002","2025-11-10","1","199.00","delivered"],
        ["ORD-011","CUST-004","PROD-001","2025-11-10","6","49.99","shipped"],
    ],
}

for date_str, rows in ORDERS.items():
    path = ORDERS_DIR / f"orders_{date_str}.csv"
    with path.open("w", newline="") as f:
        csv.writer(f).writerows(rows)
    print(f"  wrote {path}")


# ── Customers JSON (nested address) ─────────────────────────────────────────

customers = [
    {"customer_id": "CUST-001", "first_name": "Alice",  "last_name": "Smith",
     "email": "alice@example.com",  "address": {"city": "New York",  "country": "US"},
     "signup_date": "2024-01-15", "tier": "gold"},
    {"customer_id": "CUST-002", "first_name": "Bob",    "last_name": "Jones",
     "email": "bob@example.com",    "address": {"city": "London",    "country": "GB"},
     "signup_date": "2024-03-22", "tier": "standard"},
    {"customer_id": "CUST-003", "first_name": "Carol",  "last_name": "White",
     "email": "carol@example.com",  "address": {"city": "Toronto",   "country": "CA"},
     "signup_date": "2023-11-05", "tier": "silver"},
    {"customer_id": "CUST-004", "first_name": "Dan",    "last_name": "Brown",
     "email": "dan@example.com",    "address": {"city": "Sydney",    "country": "AU"},
     "signup_date": "2024-06-18", "tier": "standard"},
    # Bad row — missing @ in email → quarantine
    {"customer_id": "CUST-005", "first_name": "Eve",    "last_name": "Davis",
     "email": "eve-at-example.com", "address": {"city": "Paris",     "country": "FR"},
     "signup_date": "2024-08-01", "tier": "gold"},
]

cust_path = CUSTOMER_DIR / "customers.json"
cust_path.write_text(json.dumps(customers, indent=2))
print(f"  wrote {cust_path}")


# ── Products SQLite DB ────────────────────────────────────────────────────────

conn = sqlite3.connect(DB_PATH)
conn.execute("DROP TABLE IF EXISTS products")
conn.execute("""
    CREATE TABLE products (
        product_id  TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        category    TEXT NOT NULL,
        unit_cost   REAL NOT NULL,
        supplier_id TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
""")
products = [
    ("PROD-001", "Wireless Headphones", "Electronics", 22.50, "SUP-A", "2025-10-01T10:00:00"),
    ("PROD-002", "Laptop Stand",        "Accessories",  8.75, "SUP-B", "2025-10-15T14:30:00"),
    ("PROD-003", "USB-C Hub",           "Electronics", 12.00, "SUP-A", "2025-11-01T09:00:00"),
    ("PROD-004", "Webcam HD",           "Electronics", 35.00, "SUP-C", "2025-11-05T11:15:00"),
]
conn.executemany(
    "INSERT INTO products VALUES (?,?,?,?,?,?)", products
)
conn.commit()
conn.close()
print(f"  wrote {DB_PATH} ({len(products)} products)")

print("\nSample data generation complete.")
print("Next: python -m src.pipeline --date 2025-11-10 --backfill 3")
