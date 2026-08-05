"""Post-run Gold layer health check.

Reads the freshly written Gold artefacts for a given date and asserts:
  - Each expected file exists.
  - Row count is > 0 (fact partition and dimensions must be non-empty).
  - Key columns contain no null values.

Raises PipelineError with a descriptive message on any failure.
Logs a structured gold_health_check event regardless of outcome.
"""
from __future__ import annotations
import logging
from pathlib import Path

import pandas as pd

from src.utils.exceptions import PipelineError
from src.utils.logging_setup import log_event


# Key columns that must be non-null in each Gold table.
_KEY_COLUMNS: dict[str, list[str]] = {
    "fact_orders":  ["order_id", "customer_id", "product_id", "total_amount"],
    "dim_customer": ["customer_id", "email"],
    "dim_product":  ["product_id", "name"],
}


def run_gold_health_check(
    date_str: str,
    gold_dir: Path,
    logger: logging.Logger,
) -> None:
    """Assert Gold artefacts are present, non-empty, and free of nulls in key columns.

    Parameters
    ----------
    date_str:
        The processing date (YYYY-MM-DD).  Used to locate the fact partition.
    gold_dir:
        Root of the Gold layer (config.gold).
    logger:
        Pipeline logger.

    Raises
    ------
    PipelineError
        If any check fails.
    """
    checks: list[dict] = []
    failures: list[str] = []

    targets = {
        "fact_orders":  gold_dir / "fact_orders" / f"date={date_str}" / "data.parquet",
        "dim_customer": gold_dir / "dim_customer.parquet",
        "dim_product":  gold_dir / "dim_product.parquet",
    }

    for table, path in targets.items():
        result: dict = {"table": table, "path": str(path)}

        # 1. File exists
        if not path.exists():
            msg = f"{table}: file not found at {path}"
            result.update({"passed": False, "reason": msg})
            checks.append(result)
            failures.append(msg)
            continue

        df = pd.read_parquet(path)

        # 2. Non-empty
        if df.empty:
            msg = f"{table}: zero rows"
            result.update({"passed": False, "reason": msg, "rows": 0})
            checks.append(result)
            failures.append(msg)
            continue

        result["rows"] = len(df)

        # 3. No nulls in key columns
        null_issues: list[str] = []
        for col in _KEY_COLUMNS.get(table, []):
            if col in df.columns:
                n = int(df[col].isna().sum())
                if n > 0:
                    null_issues.append(f"{col} has {n} null(s)")
            else:
                null_issues.append(f"{col} column missing")

        if null_issues:
            msg = f"{table}: key-column issues — {'; '.join(null_issues)}"
            result.update({"passed": False, "reason": msg})
            checks.append(result)
            failures.append(msg)
        else:
            result["passed"] = True
            checks.append(result)

    passed = len(failures) == 0
    log_event(
        logger,
        "INFO" if passed else "ERROR",
        "gold_health_check",
        date=date_str,
        passed=passed,
        checks=checks,
    )

    if failures:
        raise PipelineError(
            f"Gold health check failed for {date_str}: " + "; ".join(failures)
        )
