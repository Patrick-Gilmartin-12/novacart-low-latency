"""Quick Day-4 proof-point verification script."""
import pandas as pd
from pathlib import Path

print("=== dim_date ===")
df = pd.read_parquet("data/gold/dim_date.parquet")
print(f"Rows: {len(df)}  (expect 1096 = 3 years of daily rows)")
print("Columns:", list(df.columns))
print("First:", df.iloc[0]["date_key"], "  Last:", df.iloc[-1]["date_key"])

print()
print("=== dim_product (SCD-1 proof) ===")
dfp = pd.read_parquet("data/gold/dim_product.parquet")
cost = dfp.loc[dfp["product_id"] == "PROD-001", "unit_cost"].iloc[0]
print(f"PROD-001 unit_cost = {cost}  (expect 44.99, PASS={cost == 44.99})")

print()
print("=== dim_customer (SCD-2 proof) ===")
dfc = pd.read_parquet("data/gold/dim_customer.parquet")
c2 = dfc[dfc["customer_id"] == "CUST-002"][
    ["customer_id", "city", "country", "_eff_start", "_eff_end", "_current"]
]
print(f"CUST-002 rows: {len(c2)}  (expect 2, PASS={len(c2) == 2})")
print(c2.to_string(index=False))

print()
print("=== All proof points ===")
assert len(df) == 1096,  f"dim_date row count wrong: {len(df)}"
assert cost == 44.99,    f"PROD-001 price wrong: {cost}"
assert len(c2) == 2,     f"CUST-002 SCD-2 rows wrong: {len(c2)}"
closed = c2[c2["_current"] == False]
current = c2[c2["_current"] == True]
assert len(closed) == 1 and closed.iloc[0]["city"] == "London",   "Old London row not closed"
assert len(current) == 1 and current.iloc[0]["city"] == "Portland", "New Portland row not current"
print("dim_date:    PASS (1096 rows, 2024-01-01 to 2026-12-31)")
print("SCD-1:       PASS (PROD-001 = $44.99)")
print("SCD-2:       PASS (CUST-002 London closed, Portland current)")
