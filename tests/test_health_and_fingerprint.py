"""Tests for Gold health check and schema fingerprint storage."""
from __future__ import annotations
from pathlib import Path
from datetime import date

import pandas as pd
import pytest

from src.utils.gold_health import run_gold_health_check
from src.utils.schema_fingerprint import record_fingerprint, _fingerprint
from src.utils.exceptions import PipelineError
from src.utils.logging_setup import get_logger


DATE = "2025-11-07"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def gold_dir(tmp_path: Path) -> Path:
    return tmp_path / "gold"


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    d = tmp_path / "state"
    d.mkdir()
    return d


@pytest.fixture()
def logger(tmp_path: Path):
    return get_logger("test", tmp_path / "logs")


def _write_fact(gold_dir: Path, date_str: str, rows: list[dict]) -> None:
    out = gold_dir / "fact_orders" / f"date={date_str}"
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out / "data.parquet", index=False)


def _write_dim_customer(gold_dir: Path, rows: list[dict]) -> None:
    gold_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(gold_dir / "dim_customer.parquet", index=False)


def _write_dim_product(gold_dir: Path, rows: list[dict]) -> None:
    gold_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(gold_dir / "dim_product.parquet", index=False)


GOOD_FACT = [{"order_id": "ORD-1", "customer_id": "C1",
              "product_id": "P1", "total_amount": 99.0}]
GOOD_CUSTOMER = [{"customer_id": "C1", "email": "a@b.com"}]
GOOD_PRODUCT = [{"product_id": "P1", "name": "Widget"}]


# ── Gold health check ─────────────────────────────────────────────────────────

class TestGoldHealthCheck:
    def test_passes_with_valid_gold(self, gold_dir, logger):
        _write_fact(gold_dir, DATE, GOOD_FACT)
        _write_dim_customer(gold_dir, GOOD_CUSTOMER)
        _write_dim_product(gold_dir, GOOD_PRODUCT)
        # Should not raise
        run_gold_health_check(DATE, gold_dir, logger)

    def test_fails_when_fact_missing(self, gold_dir, logger):
        _write_dim_customer(gold_dir, GOOD_CUSTOMER)
        _write_dim_product(gold_dir, GOOD_PRODUCT)
        with pytest.raises(PipelineError, match="fact_orders"):
            run_gold_health_check(DATE, gold_dir, logger)

    def test_fails_when_fact_empty(self, gold_dir, logger):
        _write_fact(gold_dir, DATE, [])
        _write_dim_customer(gold_dir, GOOD_CUSTOMER)
        _write_dim_product(gold_dir, GOOD_PRODUCT)
        with pytest.raises(PipelineError, match="zero rows"):
            run_gold_health_check(DATE, gold_dir, logger)

    def test_fails_when_key_column_has_nulls(self, gold_dir, logger):
        _write_fact(gold_dir, DATE, [
            {"order_id": None, "customer_id": "C1",
             "product_id": "P1", "total_amount": 10.0}
        ])
        _write_dim_customer(gold_dir, GOOD_CUSTOMER)
        _write_dim_product(gold_dir, GOOD_PRODUCT)
        with pytest.raises(PipelineError, match="null"):
            run_gold_health_check(DATE, gold_dir, logger)

    def test_fails_when_dim_customer_missing(self, gold_dir, logger):
        _write_fact(gold_dir, DATE, GOOD_FACT)
        _write_dim_product(gold_dir, GOOD_PRODUCT)
        with pytest.raises(PipelineError, match="dim_customer"):
            run_gold_health_check(DATE, gold_dir, logger)


# ── Schema fingerprint ────────────────────────────────────────────────────────

class TestSchemaFingerprint:
    def test_new_source_stored(self, state_dir, logger):
        import json
        record_fingerprint("orders", ["a", "b", "c"], state_dir, logger)
        stored = json.loads((state_dir / "fingerprints.json").read_text())
        assert "orders" in stored
        assert stored["orders"] == _fingerprint(["a", "b", "c"])

    def test_unchanged_does_not_overwrite(self, state_dir, logger):
        import json
        cols = ["order_id", "customer_id", "status"]
        record_fingerprint("orders", cols, state_dir, logger)
        fp_before = json.loads((state_dir / "fingerprints.json").read_text())["orders"]
        record_fingerprint("orders", cols, state_dir, logger)
        fp_after = json.loads((state_dir / "fingerprints.json").read_text())["orders"]
        assert fp_before == fp_after

    def test_changed_columns_updates_stored(self, state_dir, logger):
        import json
        record_fingerprint("orders", ["a", "b"], state_dir, logger)
        record_fingerprint("orders", ["a", "b", "c"], state_dir, logger)
        stored = json.loads((state_dir / "fingerprints.json").read_text())
        assert stored["orders"] == _fingerprint(["a", "b", "c"])

    def test_multiple_sources_stored_independently(self, state_dir, logger):
        import json
        record_fingerprint("orders", ["order_id"], state_dir, logger)
        record_fingerprint("customers", ["customer_id", "email"], state_dir, logger)
        stored = json.loads((state_dir / "fingerprints.json").read_text())
        assert "orders" in stored
        assert "customers" in stored
        assert stored["orders"] != stored["customers"]

    def test_fingerprint_is_order_independent(self):
        assert _fingerprint(["b", "a", "c"]) == _fingerprint(["c", "a", "b"])
