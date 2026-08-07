"""Bronze → Silver: validate with Pydantic, dedupe, quarantine bad rows."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Type

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.utils.schemas import OrderRow, CustomerRow, ProductRow


def _validate_df(
    df: pd.DataFrame,
    model: Type[BaseModel],
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
    date_str: str | None = None,
) -> pd.DataFrame:
    """Validate each row with Pydantic. Good rows → Silver, bad rows → quarantine."""
    good, bad = [], []
    for _, row in df.iterrows():
        try:
            validated = model(**row.to_dict())
            good.append(validated.model_dump())
        except ValidationError as exc:
            row_dict = row.to_dict()
            row_dict["_error_reason"] = str(exc)
            row_dict["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            bad.append(row_dict)

    if bad:
        if date_str:
            q_dir = quarantine_path / source_name / f"dt={date_str}"
            filename = f"{source_name}_bad.parquet"
        else:
            q_dir = quarantine_path / source_name
            filename = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.parquet"
        q_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(bad).to_parquet(q_dir / filename, index=False)
        total = len(good) + len(bad)
        log_event(logger, "WARNING", f"{source_name}_quarantined",
                  count=len(bad), total=total,
                  rate=round(len(bad) / total, 4) if total else 0)

    result = pd.DataFrame(good) if good else pd.DataFrame(columns=df.columns)

    # Deduplicate on primary key — keep last occurrence
    if primary_key in result.columns and not result.empty:
        before = len(result)
        result = result.drop_duplicates(subset=[primary_key], keep="last")
        dupes = before - len(result)
        if dupes:
            log_event(logger, "INFO", f"{source_name}_deduped", dropped=dupes)

    return result.reset_index(drop=True)


def build_silver_orders(
    date_str: str,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "orders" / f"date={date_str}" / "data.parquet"
    if not src.exists():
        raise IngestionError(f"bronze orders not found for {date_str}: {src}")

    df = pd.read_parquet(src)
    df = _validate_df(df, OrderRow, "order_id", quarantine_dir, logger, "orders", date_str)

    out_dir = silver_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_orders_written", rows=len(df), date=date_str)
    return out_path


def build_silver_customers(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
    date_str: str | None = None,
) -> Path:
    src = bronze_dir / "customers" / "data.parquet"
    if not src.exists():
        raise IngestionError(f"bronze customers not found: {src}")

    df = pd.read_parquet(src)
    df = _validate_df(df, CustomerRow, "customer_id", quarantine_dir, logger, "customers", date_str)

    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_customers_written", rows=len(df))
    return out_path


def build_silver_products(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "products" / "data.parquet"
    if not src.exists():
        raise IngestionError(f"bronze products not found: {src}")

    df = pd.read_parquet(src)
    if df.empty:
        return silver_dir / "products" / "data.parquet"

    df = _validate_df(df, ProductRow, "product_id", quarantine_dir, logger, "products")

    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_products_written", rows=len(df))
    return out_path
