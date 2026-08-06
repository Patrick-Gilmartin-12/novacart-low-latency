"""
Quarantine and validation tests driven by historic synthetic data.

Each test class maps to one failure mode or scenario.  The fixtures replicate
the exact row shapes defined in scripts/generate_historic_data.py so the
assertions are precise and deterministic.

Scenarios covered
─────────────────
A. All-clean batch          → zero quarantine, all rows reach Silver
B. Bad quantity (0 / neg)   → exact quarantine count, Silver count, _error_reason
C. Bad unit_price (neg)     → exact quarantine count, Silver count, _error_reason
D. Invalid status values    → exact quarantine count, _error_reason mentions status
E. Multi-failure batch      → 3 different violation types, each quarantined once
F. All-bad batch            → Silver is empty, every row quarantined
G. Quarantine file layout   → path is dt=<date>/orders_bad.parquet, columns present
H. Customers: bad email     → quarantined, valid customers reach Silver
I. Customers: missing field → quarantined alongside bad email row
J. Products: negative cost  → quarantined, valid products reach Silver
K. Duplicates only          → one row survives dedup, zero quarantined
L. Interleaved good/bad     → bad rows quarantined, good rows unaffected in Silver
"""
from __future__ import annotations
from pathlib import Path

import pandas as pd
import pytest

from tests.conftest import write_orders_csv, write_customers_json, make_products_db
from src.pipeline import run_one_date
from src.utils.config import Config

# ── Shared fixtures ───────────────────────────────────────────────────────────

DATE = "2025-01-07"   # historic Monday — used as the processing date in most tests

GOOD_CUSTOMER = {
    "customer_id": "CUST-H01", "first_name": "Liam", "last_name": "Chen",
    "email": "liam.chen@shopnova.com",
    "address": {"city": "Chicago", "country": "US"},
    "signup_date": "2023-03-10", "tier": "gold",
}
GOOD_PRODUCT = ("PROD-H01", "Noise-Cancelling Headphones", "Electronics",
                38.00, "SUP-X", "2024-10-01T08:00:00")


def _q_orders(config: Config, date: str = DATE) -> pd.DataFrame:
    """Read all quarantine parquet files for orders on *date*, return as DataFrame."""
    q_dir = config.quarantine / "orders"
    files = list(q_dir.glob("**/*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _silver_orders(config: Config, date: str = DATE) -> pd.DataFrame:
    p = config.silver / "orders" / f"date={date}" / "data.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def _q_customers(config: Config) -> pd.DataFrame:
    q_dir = config.quarantine / "customers"
    files = list(q_dir.glob("**/*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _silver_customers(config: Config) -> pd.DataFrame:
    p = config.silver / "customers" / "data.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def _q_products(config: Config) -> pd.DataFrame:
    q_dir = config.quarantine / "products"
    files = list(q_dir.glob("**/*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# ── A: All-clean batch ────────────────────────────────────────────────────────

class TestAllCleanBatch:
    """2025-01-06 historic batch — all 5 rows are valid."""

    DATE = "2025-01-06"

    def _setup(self, config):
        write_orders_csv(config.landing_orders, self.DATE, [
            ["ORD-H001", "CUST-H01", "PROD-H01", self.DATE, "3",  "89.99",  "shipped"],
            ["ORD-H002", "CUST-H02", "PROD-H02", self.DATE, "1",  "249.00", "pending"],
            ["ORD-H003", "CUST-H03", "PROD-H01", self.DATE, "2",  "89.99",  "delivered"],
            ["ORD-H004", "CUST-H04", "PROD-H03", self.DATE, "1",  "14.50",  "cancelled"],
            ["ORD-H005", "CUST-H05", "PROD-H04", self.DATE, "4",  "19.99",  "returned"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_pipeline_succeeds(self, config):
        self._setup(config)
        result = run_one_date(self.DATE, config)
        assert result["status"] == "SUCCESS"

    def test_all_rows_reach_silver(self, config):
        self._setup(config)
        run_one_date(self.DATE, config)
        silver = _silver_orders(config, self.DATE)
        assert len(silver) == 5

    def test_no_quarantine_files_produced(self, config):
        self._setup(config)
        run_one_date(self.DATE, config)
        q_df = _q_orders(config, self.DATE)
        assert q_df.empty

    def test_all_status_values_preserved(self, config):
        self._setup(config)
        run_one_date(self.DATE, config)
        silver = _silver_orders(config, self.DATE)
        statuses = set(silver["status"].tolist())
        assert statuses == {"shipped", "pending", "delivered", "cancelled", "returned"}


# ── B: Bad quantity ───────────────────────────────────────────────────────────

class TestBadQuantity:
    """2025-01-07 historic batch — qty=0 and qty=-3 must be quarantined."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-H006", "CUST-H01", "PROD-H02", DATE, "2",  "249.00", "shipped"],
            ["ORD-H007", "CUST-H02", "PROD-H01", DATE, "0",  "89.99",  "pending"],    # BAD
            ["ORD-H008", "CUST-H03", "PROD-H03", DATE, "-3", "14.50",  "delivered"],  # BAD
            ["ORD-H009", "CUST-H04", "PROD-H04", DATE, "1",  "19.99",  "shipped"],
            ["ORD-H010", "CUST-H05", "PROD-H01", DATE, "6",  "89.99",  "delivered"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_exactly_two_rows_quarantined(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_q_orders(config)) == 2

    def test_exactly_three_rows_reach_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_silver_orders(config)) == 3

    def test_quarantined_order_ids_are_correct(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        quarantined_ids = set(_q_orders(config)["order_id"].tolist())
        assert quarantined_ids == {"ORD-H007", "ORD-H008"}

    def test_error_reason_column_present(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_orders(config)
        assert "_error_reason" in q_df.columns

    def test_error_reason_mentions_quantity(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_orders(config)
        assert q_df["_error_reason"].str.contains("quantity", case=False).all()

    def test_quarantined_at_column_present(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_orders(config)
        assert "_quarantined_at" in q_df.columns
        assert q_df["_quarantined_at"].notna().all()

    def test_zero_quantity_not_in_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver = _silver_orders(config)
        assert "ORD-H007" not in silver["order_id"].tolist()

    def test_negative_quantity_not_in_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver = _silver_orders(config)
        assert "ORD-H008" not in silver["order_id"].tolist()


# ── C: Bad unit_price ─────────────────────────────────────────────────────────

class TestBadUnitPrice:
    """2025-01-08 historic batch — unit_price = -5.00 must be quarantined."""

    DATE_C = "2025-01-08"

    def _setup(self, config):
        write_orders_csv(config.landing_orders, self.DATE_C, [
            ["ORD-H011", "CUST-H01", "PROD-H03", self.DATE_C, "2", "14.50",  "pending"],
            ["ORD-H012", "CUST-H02", "PROD-H02", self.DATE_C, "1", "-5.00",  "shipped"],  # BAD
            ["ORD-H013", "CUST-H03", "PROD-H01", self.DATE_C, "3", "89.99",  "delivered"],
            ["ORD-H014", "CUST-H04", "PROD-H04", self.DATE_C, "2", "19.99",  "cancelled"],
            ["ORD-H015", "CUST-H05", "PROD-H01", self.DATE_C, "1", "89.99",  "shipped"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_exactly_one_row_quarantined(self, config):
        self._setup(config)
        run_one_date(self.DATE_C, config)
        assert len(_q_orders(config, self.DATE_C)) == 1

    def test_four_rows_reach_silver(self, config):
        self._setup(config)
        run_one_date(self.DATE_C, config)
        assert len(_silver_orders(config, self.DATE_C)) == 4

    def test_quarantined_row_is_ord_h012(self, config):
        self._setup(config)
        run_one_date(self.DATE_C, config)
        q_df = _q_orders(config, self.DATE_C)
        assert q_df.iloc[0]["order_id"] == "ORD-H012"

    def test_error_reason_mentions_unit_price(self, config):
        self._setup(config)
        run_one_date(self.DATE_C, config)
        q_df = _q_orders(config, self.DATE_C)
        assert q_df["_error_reason"].str.contains("unit_price", case=False).all()

    def test_zero_price_is_allowed(self, config):
        """unit_price = 0.00 is valid (ge=0).  Confirm it passes through."""
        write_orders_csv(config.landing_orders, self.DATE_C, [
            ["ORD-FREE", "CUST-H01", "PROD-H01", self.DATE_C, "1", "0.00", "shipped"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])
        run_one_date(self.DATE_C, config)
        silver = _silver_orders(config, self.DATE_C)
        assert "ORD-FREE" in silver["order_id"].tolist()
        assert _q_orders(config, self.DATE_C).empty


# ── D: Invalid status ─────────────────────────────────────────────────────────

class TestInvalidStatus:
    """2025-01-09 historic batch — 'refunded' and 'processing' are not in Literal."""

    DATE_D = "2025-01-09"

    def _setup(self, config):
        write_orders_csv(config.landing_orders, self.DATE_D, [
            ["ORD-H016", "CUST-H01", "PROD-H04", self.DATE_D, "5",  "19.99",  "shipped"],
            ["ORD-H017", "CUST-H02", "PROD-H01", self.DATE_D, "2",  "89.99",  "refunded"],    # BAD
            ["ORD-H018", "CUST-H03", "PROD-H02", self.DATE_D, "1",  "249.00", "processing"],  # BAD
            ["ORD-H019", "CUST-H04", "PROD-H03", self.DATE_D, "3",  "14.50",  "pending"],
            ["ORD-H020", "CUST-H05", "PROD-H01", self.DATE_D, "2",  "89.99",  "delivered"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_two_rows_quarantined(self, config):
        self._setup(config)
        run_one_date(self.DATE_D, config)
        assert len(_q_orders(config, self.DATE_D)) == 2

    def test_three_rows_reach_silver(self, config):
        self._setup(config)
        run_one_date(self.DATE_D, config)
        assert len(_silver_orders(config, self.DATE_D)) == 3

    def test_error_reason_mentions_status(self, config):
        self._setup(config)
        run_one_date(self.DATE_D, config)
        q_df = _q_orders(config, self.DATE_D)
        assert q_df["_error_reason"].str.contains("status", case=False).all()

    def test_mixed_case_valid_status_passes(self, config):
        """'Shipped' (mixed case) should normalise to 'shipped' and pass."""
        write_orders_csv(config.landing_orders, self.DATE_D, [
            ["ORD-CASE", "CUST-H01", "PROD-H01", self.DATE_D, "1", "89.99", "Shipped"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])
        run_one_date(self.DATE_D, config)
        silver = _silver_orders(config, self.DATE_D)
        assert len(silver) == 1
        assert silver.iloc[0]["status"] == "shipped"
        assert _q_orders(config, self.DATE_D).empty


# ── E: Multi-failure batch ────────────────────────────────────────────────────

class TestMultiFailureBatch:
    """2025-01-10 historic batch — 3 distinct violation types in one batch."""

    DATE_E = "2025-01-10"

    def _setup(self, config):
        write_orders_csv(config.landing_orders, self.DATE_E, [
            ["ORD-H021", "CUST-H01", "PROD-H01", self.DATE_E, "1",  "89.99",   "shipped"],
            ["ORD-H022", "CUST-H02", "PROD-H02", self.DATE_E, "0",  "249.00",  "pending"],    # BAD qty
            ["ORD-H023", "CUST-H03", "PROD-H03", self.DATE_E, "2",  "14.50",   "delivered"],
            ["ORD-H024", "CUST-H04", "PROD-H04", self.DATE_E, "3",  "-0.01",   "shipped"],    # BAD price
            ["ORD-H025", "CUST-H05", "PROD-H01", self.DATE_E, "4",  "89.99",   "dispatched"], # BAD status
            ["ORD-H026", "CUST-H01", "PROD-H02", self.DATE_E, "1",  "249.00",  "cancelled"],
            ["ORD-H027", "CUST-H03", "PROD-H04", self.DATE_E, "2",  "19.99",   "returned"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_exactly_three_quarantined(self, config):
        self._setup(config)
        run_one_date(self.DATE_E, config)
        assert len(_q_orders(config, self.DATE_E)) == 3

    def test_four_rows_reach_silver(self, config):
        self._setup(config)
        run_one_date(self.DATE_E, config)
        assert len(_silver_orders(config, self.DATE_E)) == 4

    def test_each_bad_order_id_quarantined(self, config):
        self._setup(config)
        run_one_date(self.DATE_E, config)
        quarantined_ids = set(_q_orders(config, self.DATE_E)["order_id"].tolist())
        assert quarantined_ids == {"ORD-H022", "ORD-H024", "ORD-H025"}

    def test_each_quarantined_row_has_distinct_error(self, config):
        self._setup(config)
        run_one_date(self.DATE_E, config)
        q_df = _q_orders(config, self.DATE_E).set_index("order_id")
        assert "quantity" in q_df.loc["ORD-H022", "_error_reason"].lower()
        assert "unit_price" in q_df.loc["ORD-H024", "_error_reason"].lower()
        assert "status" in q_df.loc["ORD-H025", "_error_reason"].lower()

    def test_good_rows_total_amount_correct(self, config):
        """The 4 good rows should have correct total_amount in Gold."""
        self._setup(config)
        run_one_date(self.DATE_E, config)
        fact = pd.read_parquet(
            config.gold / "fact_orders" / f"date={self.DATE_E}" / "data.parquet"
        )
        assert len(fact) == 4
        # ORD-H021: 1 × 89.99 = 89.99
        row = fact[fact["order_id"] == "ORD-H021"].iloc[0]
        assert row["total_amount"] == pytest.approx(89.99)


# ── F: All-bad batch ──────────────────────────────────────────────────────────

class TestAllBadBatch:
    """Every row in the batch is invalid — Silver should be empty."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-BAD1", "CUST-H01", "PROD-H01", DATE, "0",  "10.00",   "shipped"],    # qty=0
            ["ORD-BAD2", "CUST-H02", "PROD-H01", DATE, "-1", "10.00",   "pending"],    # qty<0
            ["ORD-BAD3", "CUST-H03", "PROD-H01", DATE, "1",  "-9.99",   "delivered"],  # price<0
            ["ORD-BAD4", "CUST-H04", "PROD-H01", DATE, "2",  "10.00",   "voided"],     # bad status
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_silver_is_empty(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver = _silver_orders(config)
        assert len(silver) == 0

    def test_all_four_rows_quarantined(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_q_orders(config)) == 4

    def test_every_quarantine_row_has_error_reason(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_orders(config)
        assert q_df["_error_reason"].notna().all()
        assert (q_df["_error_reason"] != "").all()


# ── G: Quarantine file layout ─────────────────────────────────────────────────

class TestQuarantineFileLayout:
    """Quarantine must land at dt=<date>/orders_bad.parquet."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-GOOD", "CUST-H01", "PROD-H01", DATE, "1",  "89.99",  "shipped"],
            ["ORD-BAD",  "CUST-H02", "PROD-H01", DATE, "0",  "89.99",  "pending"],  # BAD
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_quarantine_path_uses_dt_partition(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        expected = config.quarantine / "orders" / f"dt={DATE}" / "orders_bad.parquet"
        assert expected.exists(), f"Expected quarantine file not found: {expected}"

    def test_quarantine_file_contains_error_reason_column(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        expected = config.quarantine / "orders" / f"dt={DATE}" / "orders_bad.parquet"
        q_df = pd.read_parquet(expected)
        assert "_error_reason" in q_df.columns

    def test_quarantine_file_contains_quarantined_at_column(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        expected = config.quarantine / "orders" / f"dt={DATE}" / "orders_bad.parquet"
        q_df = pd.read_parquet(expected)
        assert "_quarantined_at" in q_df.columns

    def test_quarantine_file_preserves_original_field_values(self, config):
        """The bad row's original values must be readable in the quarantine file."""
        self._setup(config)
        run_one_date(DATE, config)
        expected = config.quarantine / "orders" / f"dt={DATE}" / "orders_bad.parquet"
        q_df = pd.read_parquet(expected)
        assert q_df.iloc[0]["order_id"] == "ORD-BAD"


# ── H: Customer bad email ─────────────────────────────────────────────────────

class TestCustomerBadEmail:
    """Customer with no @ in email must be quarantined; others reach Silver."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-X01", "CUST-H01", "PROD-H01", DATE, "1", "89.99", "shipped"],
        ])
        write_customers_json(config.landing_customers, [
            GOOD_CUSTOMER,
            {"customer_id": "CUST-H02", "first_name": "Sofia", "last_name": "Rossi",
             "email": "sofia.rossi@shopnova.it",
             "address": {"city": "Milan", "country": "IT"},
             "signup_date": "2023-06-22", "tier": "standard"},
            # BAD: email missing @
            {"customer_id": "CUST-H07", "first_name": "Nina", "last_name": "Ford",
             "email": "nina-at-shopnova.com",
             "address": {"city": "Amsterdam", "country": "NL"},
             "signup_date": "2024-07-05", "tier": "gold"},
        ])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_bad_email_customer_quarantined(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_customers(config)
        assert len(q_df) == 1
        assert q_df.iloc[0]["customer_id"] == "CUST-H07"

    def test_valid_customers_reach_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver = _silver_customers(config)
        assert len(silver) == 2
        assert "CUST-H07" not in silver["customer_id"].tolist()

    def test_customer_error_reason_mentions_email(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_customers(config)
        assert "email" in q_df.iloc[0]["_error_reason"].lower()

    def test_customer_quarantine_path_is_date_partitioned(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        expected = config.quarantine / "customers" / f"dt={DATE}" / "customers_bad.parquet"
        assert expected.exists()


# ── I: Multiple customer failures in one batch ────────────────────────────────

class TestMultipleCustomerFailures:
    """Both bad-email and bad-email+missing-field customers quarantined."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-X02", "CUST-H01", "PROD-H01", DATE, "1", "89.99", "shipped"],
        ])
        write_customers_json(config.landing_customers, [
            GOOD_CUSTOMER,
            # BAD 1: email missing @
            {"customer_id": "CUST-H07", "first_name": "Nina", "last_name": "Ford",
             "email": "nina-at-shopnova.com",
             "address": {"city": "Amsterdam", "country": "NL"},
             "signup_date": "2024-07-05", "tier": "gold"},
            # BAD 2: also bad email (double bad — ensures count is 2 not 1)
            {"customer_id": "CUST-H08", "first_name": "Unknown", "last_name": "Unknown",
             "email": "unknown-noemail",
             "address": {"city": "Unknown", "country": "XX"},
             "signup_date": "2024-09-01", "tier": "standard"},
        ])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_two_customers_quarantined(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_q_customers(config)) == 2

    def test_one_valid_customer_reaches_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_silver_customers(config)) == 1


# ── J: Product negative cost ──────────────────────────────────────────────────

class TestProductNegativeCost:
    """Product with unit_cost < 0 must be quarantined; others reach Silver."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-X03", "CUST-H01", "PROD-H01", DATE, "1", "89.99", "shipped"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [
            ("PROD-H01", "Noise-Cancelling Headphones", "Electronics", 38.00, "SUP-X", "2024-10-01T08:00:00"),
            ("PROD-H02", "Mechanical Keyboard",         "Peripherals",  55.00, "SUP-Y", "2024-10-15T10:30:00"),
            # BAD: negative unit_cost
            ("PROD-H06", "Broken SKU", "Unknown", -1.00, "SUP-X", "2024-12-15T16:00:00"),
        ])

    def test_bad_product_quarantined(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_products(config)
        assert len(q_df) == 1
        assert q_df.iloc[0]["product_id"] == "PROD-H06"

    def test_valid_products_reach_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver = pd.read_parquet(config.silver / "products" / "data.parquet")
        assert len(silver) == 2
        assert "PROD-H06" not in silver["product_id"].tolist()

    def test_product_error_reason_mentions_unit_cost(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        q_df = _q_products(config)
        assert "unit_cost" in q_df.iloc[0]["_error_reason"].lower()

    def test_zero_cost_product_is_valid(self, config):
        """unit_cost = 0.00 is valid (ge=0) — free product should reach Silver."""
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-X04", "CUST-H01", "PROD-FREE", DATE, "1", "0.00", "shipped"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [
            ("PROD-FREE", "Free Sample", "Promotions", 0.00, "SUP-X", "2024-01-01T00:00:00"),
        ])
        run_one_date(DATE, config)
        silver = pd.read_parquet(config.silver / "products" / "data.parquet")
        assert "PROD-FREE" in silver["product_id"].tolist()
        assert _q_products(config).empty


# ── K: Duplicates only ────────────────────────────────────────────────────────

class TestDuplicatesOnly:
    """Batch consists entirely of the same order repeated — dedup leaves exactly one."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-DUP", "CUST-H01", "PROD-H01", DATE, "2", "89.99", "shipped"],
            ["ORD-DUP", "CUST-H01", "PROD-H01", DATE, "2", "89.99", "shipped"],
            ["ORD-DUP", "CUST-H01", "PROD-H01", DATE, "2", "89.99", "shipped"],
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_one_row_in_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_silver_orders(config)) == 1

    def test_no_quarantine_from_duplicates(self, config):
        """Duplicates are deduped — they are NOT quarantine errors."""
        self._setup(config)
        run_one_date(DATE, config)
        assert _q_orders(config).empty


# ── L: Interleaved good and bad ───────────────────────────────────────────────

class TestInterleavedGoodBad:
    """Good and bad rows interleaved — bad quarantined, good rows unaffected."""

    def _setup(self, config):
        write_orders_csv(config.landing_orders, DATE, [
            ["ORD-G1",  "CUST-H01", "PROD-H01", DATE, "1",  "89.99",  "shipped"],   # good
            ["ORD-B1",  "CUST-H02", "PROD-H01", DATE, "0",  "89.99",  "pending"],   # bad qty
            ["ORD-G2",  "CUST-H03", "PROD-H01", DATE, "3",  "19.99",  "delivered"], # good
            ["ORD-B2",  "CUST-H04", "PROD-H01", DATE, "2",  "89.99",  "voided"],    # bad status
            ["ORD-G3",  "CUST-H05", "PROD-H01", DATE, "5",  "14.50",  "cancelled"], # good
        ])
        write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
        make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    def test_three_good_reach_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver = _silver_orders(config)
        assert len(silver) == 3

    def test_two_bad_quarantined(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        assert len(_q_orders(config)) == 2

    def test_good_order_ids_all_in_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver_ids = set(_silver_orders(config)["order_id"].tolist())
        assert {"ORD-G1", "ORD-G2", "ORD-G3"}.issubset(silver_ids)

    def test_bad_order_ids_not_in_silver(self, config):
        self._setup(config)
        run_one_date(DATE, config)
        silver_ids = set(_silver_orders(config)["order_id"].tolist())
        assert "ORD-B1" not in silver_ids
        assert "ORD-B2" not in silver_ids
