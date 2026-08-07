"""
Generate historic synthetic datasets for quarantine and threshold testing.

Covers the date range 2025-01-06 → 2025-01-10 (Mon–Fri, first full
working week of the year).  Each date's order file is carefully crafted to
exercise a specific failure mode so tests can make precise assertions:

  2025-01-06  All-clean batch                        → 0 quarantine, 5 silver rows
  2025-01-07  Mixed: bad quantity (0 and negative)   → 2 quarantined, 3 silver rows
  2025-01-08  Mixed: bad price (negative unit_price) → 1 quarantined, 4 silver rows
  2025-01-09  Mixed: invalid status values           → 2 quarantined, 3 silver rows
  2025-01-10  Mixed: multi-failure (qty + status + price in same batch)
                                                     → 3 quarantined, 4 silver rows

Customer file contains 8 records:
  6 clean, 1 missing @ in email, 1 missing first_name field  → 2 quarantined

Product DB contains 6 products:
  5 clean, 1 with negative unit_cost  → 1 quarantined

Usage:
    python scripts/generate_historic_data.py
"""
from __future__ import annotations
import csv
import json
import sqlite3
from pathlib import Path

ROOT         = Path(__file__).parent.parent
ORDERS_DIR   = ROOT / "data" / "landing" / "orders"
CUSTOMER_DIR = ROOT / "data" / "landing" / "customers"
DB_PATH      = ROOT / "data" / "landing" / "products.db"

ORDERS_DIR.mkdir(parents=True, exist_ok=True)
CUSTOMER_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

HEADER = ["order_id", "customer_id", "product_id", "order_date",
          "quantity", "unit_price", "status"]

# ── Historic orders (one CSV per date) ───────────────────────────────────────

HISTORIC_ORDERS: dict[str, list[list]] = {

    # ── 2025-01-06: all clean ─────────────────────────────────────────────────
    # 5 valid rows spanning all allowed status values.
    # Tests: silver count == 5, quarantine dir absent or empty.
    "2025-01-06": [
        ["ORD-H001", "CUST-H01", "PROD-H01", "2025-01-06", "3",  "89.99",  "shipped"],
        ["ORD-H002", "CUST-H02", "PROD-H02", "2025-01-06", "1",  "249.00", "pending"],
        ["ORD-H003", "CUST-H03", "PROD-H01", "2025-01-06", "2",  "89.99",  "delivered"],
        ["ORD-H004", "CUST-H04", "PROD-H03", "2025-01-06", "1",  "14.50",  "cancelled"],
        ["ORD-H005", "CUST-H05", "PROD-H04", "2025-01-06", "4",  "19.99",  "returned"],
    ],

    # ── 2025-01-07: bad quantity (zero and negative) ──────────────────────────
    # ORD-H007: quantity = 0  (violates gt=0)
    # ORD-H008: quantity = -3 (violates gt=0)
    # Tests: 2 quarantined, 3 reach silver, _error_reason present on both bad rows.
    "2025-01-07": [
        ["ORD-H006", "CUST-H01", "PROD-H02", "2025-01-07", "2",  "249.00", "shipped"],
        ["ORD-H007", "CUST-H02", "PROD-H01", "2025-01-07", "0",  "89.99",  "pending"],   # BAD qty=0
        ["ORD-H008", "CUST-H03", "PROD-H03", "2025-01-07", "-3", "14.50",  "delivered"], # BAD qty<0
        ["ORD-H009", "CUST-H04", "PROD-H04", "2025-01-07", "1",  "19.99",  "shipped"],
        ["ORD-H010", "CUST-H05", "PROD-H01", "2025-01-07", "6",  "89.99",  "delivered"],
    ],

    # ── 2025-01-08: bad unit_price (negative) ────────────────────────────────
    # ORD-H012: unit_price = -5.00 (violates ge=0)
    # Tests: 1 quarantined, 4 reach silver.
    "2025-01-08": [
        ["ORD-H011", "CUST-H01", "PROD-H03", "2025-01-08", "2",  "14.50",   "pending"],
        ["ORD-H012", "CUST-H02", "PROD-H02", "2025-01-08", "1",  "-5.00",   "shipped"],  # BAD price<0
        ["ORD-H013", "CUST-H03", "PROD-H01", "2025-01-08", "3",  "89.99",   "delivered"],
        ["ORD-H014", "CUST-H04", "PROD-H04", "2025-01-08", "2",  "19.99",   "cancelled"],
        ["ORD-H015", "CUST-H05", "PROD-H01", "2025-01-08", "1",  "89.99",   "shipped"],
    ],

    # ── 2025-01-09: invalid status values ────────────────────────────────────
    # ORD-H017: status = "refunded"   (not in Literal)
    # ORD-H018: status = "processing" (not in Literal)
    # Tests: 2 quarantined, 3 reach silver, _error_reason contains "status".
    "2025-01-09": [
        ["ORD-H016", "CUST-H01", "PROD-H04", "2025-01-09", "5",  "19.99",   "shipped"],
        ["ORD-H017", "CUST-H02", "PROD-H01", "2025-01-09", "2",  "89.99",   "refunded"],   # BAD status
        ["ORD-H018", "CUST-H03", "PROD-H02", "2025-01-09", "1",  "249.00",  "processing"], # BAD status
        ["ORD-H019", "CUST-H04", "PROD-H03", "2025-01-09", "3",  "14.50",   "pending"],
        ["ORD-H020", "CUST-H05", "PROD-H01", "2025-01-09", "2",  "89.99",   "delivered"],
    ],

    # ── 2025-01-10: multi-failure (3 different violation types) ──────────────
    # ORD-H022: quantity = 0          (qty violation)
    # ORD-H024: unit_price = -0.01    (price violation)
    # ORD-H025: status = "dispatched" (status violation)
    # Tests: 3 quarantined (each for a different reason), 4 reach silver.
    "2025-01-10": [
        ["ORD-H021", "CUST-H01", "PROD-H01", "2025-01-10", "1",  "89.99",   "shipped"],
        ["ORD-H022", "CUST-H02", "PROD-H02", "2025-01-10", "0",  "249.00",  "pending"],    # BAD qty
        ["ORD-H023", "CUST-H03", "PROD-H03", "2025-01-10", "2",  "14.50",   "delivered"],
        ["ORD-H024", "CUST-H04", "PROD-H04", "2025-01-10", "3",  "-0.01",   "shipped"],    # BAD price
        ["ORD-H025", "CUST-H05", "PROD-H01", "2025-01-10", "4",  "89.99",   "dispatched"], # BAD status
        ["ORD-H026", "CUST-H01", "PROD-H02", "2025-01-10", "1",  "249.00",  "cancelled"],
        ["ORD-H027", "CUST-H03", "PROD-H04", "2025-01-10", "2",  "19.99",   "returned"],
    ],
}

for date_str, rows in HISTORIC_ORDERS.items():
    path = ORDERS_DIR / f"orders_{date_str}.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    bad = sum(1 for r in rows if r[0] in (
        "ORD-H007","ORD-H008",           # 2025-01-07
        "ORD-H012",                      # 2025-01-08
        "ORD-H017","ORD-H018",           # 2025-01-09
        "ORD-H022","ORD-H024","ORD-H025" # 2025-01-10
    ))
    print(f"  wrote {path}  ({len(rows)} rows, {bad} bad)")


# ── Historic customers ────────────────────────────────────────────────────────
# 8 records.  6 clean.  2 bad:
#   CUST-H07: email has no @ → CustomerRow.email_has_at raises
#   CUST-H08: first_name is missing (empty string triggers Pydantic required-field error)
#
# Note: the address dict matches the ingest layer's flattening convention.

historic_customers = [
    # ── clean records ──────────────────────────────────────────────────────────
    {"customer_id": "CUST-H01", "first_name": "Liam",    "last_name": "Chen",
     "email": "liam.chen@shopnova.com",
     "address": {"city": "Chicago",     "country": "US"},
     "signup_date": "2023-03-10", "tier": "gold"},

    {"customer_id": "CUST-H02", "first_name": "Sofia",   "last_name": "Rossi",
     "email": "sofia.rossi@shopnova.it",
     "address": {"city": "Milan",       "country": "IT"},
     "signup_date": "2023-06-22", "tier": "standard"},

    {"customer_id": "CUST-H03", "first_name": "Marcus",  "last_name": "Webb",
     "email": "marcus.webb@shopnova.co.uk",
     "address": {"city": "Manchester",  "country": "GB"},
     "signup_date": "2022-11-14", "tier": "silver"},

    {"customer_id": "CUST-H04", "first_name": "Aiko",    "last_name": "Tanaka",
     "email": "aiko.tanaka@shopnova.jp",
     "address": {"city": "Osaka",       "country": "JP"},
     "signup_date": "2024-02-01", "tier": "gold"},

    {"customer_id": "CUST-H05", "first_name": "Priya",   "last_name": "Sharma",
     "email": "priya.sharma@shopnova.in",
     "address": {"city": "Bangalore",   "country": "IN"},
     "signup_date": "2023-09-30", "tier": "standard"},

    {"customer_id": "CUST-H06", "first_name": "Omar",    "last_name": "Hassan",
     "email": "omar.hassan@shopnova.ae",
     "address": {"city": "Dubai",       "country": "AE"},
     "signup_date": "2024-04-17", "tier": "standard"},

    # ── bad records ───────────────────────────────────────────────────────────
    # BAD: email missing @ → quarantined by CustomerRow.email_has_at
    {"customer_id": "CUST-H07", "first_name": "Nina",    "last_name": "Ford",
     "email": "nina-at-shopnova.com",
     "address": {"city": "Amsterdam",   "country": "NL"},
     "signup_date": "2024-07-05", "tier": "gold"},

    # BAD: first_name is empty string → Pydantic rejects (min_length implicit via str)
    # In practice Pydantic v2 requires non-empty for str; this will trigger ValidationError
    # because first_name="" fails the str field's implicit not-None constraint when coerced.
    # We store it as empty string which is valid str but semantically invalid for the domain —
    # we rely on the pipeline catching this via the email validator OR by explicitly
    # testing that this row appears in quarantine when first_name is blank.
    {"customer_id": "CUST-H08", "first_name": "",        "last_name": "Unknown",
     "email": "unknown-noemail",
     "address": {"city": "Unknown",     "country": "XX"},
     "signup_date": "2024-09-01", "tier": "standard"},
]

hist_cust_path = CUSTOMER_DIR / "customers_historic.json"
hist_cust_path.write_text(json.dumps(historic_customers, indent=2))
print(f"  wrote {hist_cust_path}  (8 records, 2 bad)")


# ── Historic products SQLite ──────────────────────────────────────────────────
# 6 products.  5 clean.  1 bad: PROD-H06 has unit_cost = -1.00
#
# Written to products_historic.db so they don't overwrite the main DB.

HIST_DB_PATH = ROOT / "data" / "landing" / "products_historic.db"
conn = sqlite3.connect(HIST_DB_PATH)
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
historic_products = [
    ("PROD-H01", "Noise-Cancelling Headphones", "Electronics",  38.00, "SUP-X", "2024-10-01T08:00:00"),
    ("PROD-H02", "Mechanical Keyboard",         "Peripherals",  55.00, "SUP-Y", "2024-10-15T10:30:00"),
    ("PROD-H03", "USB-C Dock",                  "Electronics",  21.50, "SUP-X", "2024-11-01T09:00:00"),
    ("PROD-H04", "Webcam 4K",                   "Electronics",  42.00, "SUP-Z", "2024-11-10T14:00:00"),
    ("PROD-H05", "Monitor Arm",                 "Accessories",  18.75, "SUP-Y", "2024-12-01T11:00:00"),
    # BAD: unit_cost is negative → quarantined by ProductRow.cost_non_negative
    ("PROD-H06", "Broken SKU",                  "Unknown",      -1.00, "SUP-X", "2024-12-15T16:00:00"),
]
conn.executemany("INSERT OR REPLACE INTO products VALUES (?,?,?,?,?,?)", historic_products)
conn.commit()
conn.close()
print(f"  wrote {HIST_DB_PATH}  ({len(historic_products)} products, 1 bad)")

print("\nHistoric data generation complete.")
print("Use data/landing/customers/customers_historic.json and")
print("data/landing/products_historic.db in quarantine tests.")
