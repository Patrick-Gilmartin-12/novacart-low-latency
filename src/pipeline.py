"""
NovaCart ETL — CLI Orchestrator
Usage:
    python -m src.pipeline --date 2025-11-07
    python -m src.pipeline --date 2025-11-10 --backfill 3
    python -m src.pipeline --date 2025-11-07 --inspect
"""
from __future__ import annotations
import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.utils.config import Config
from src.utils.logging_setup import get_logger, log_event
from src.utils.state import StateManager
from src.ingest.orders import ingest_orders
from src.ingest.customers import ingest_customers
from src.ingest.products import ingest_products
from src.transform.silver import (
    build_silver_orders,
    build_silver_customers,
    build_silver_products,
)
from src.transform.gold import (
    build_dim_date,
    build_dim_product,
    build_dim_customer,
    build_fact_orders,
)
from src.utils.gold_health import run_gold_health_check
from src.utils.schema_fingerprint import record_fingerprint


def run_one_date(date_str: str, config: Config) -> dict:
    run_id = str(uuid.uuid4())
    logger = get_logger("novacart", config.logs)
    state = StateManager(config.state)
    started_at = datetime.now(timezone.utc)
    stages: list[dict] = []

    log_event(logger, "INFO", "pipeline_start", run_id=run_id, date=date_str)

    def stage(name: str, fn):
        t0 = datetime.utcnow()
        log_event(logger, "INFO", "stage_start", run_id=run_id, stage=name)
        try:
            fn()
            stages.append({"stage": name, "status": "OK",
                           "duration_sec": (datetime.utcnow() - t0).total_seconds()})
            log_event(logger, "INFO", "stage_end", run_id=run_id, stage=name, status="OK")
        except Exception as exc:
            stages.append({"stage": name, "status": "FAIL", "error": str(exc),
                           "duration_sec": (datetime.utcnow() - t0).total_seconds()})
            log_event(logger, "ERROR", "stage_end", run_id=run_id, stage=name,
                      status="FAIL", error=str(exc))
            raise

    status, error_msg = "SUCCESS", None
    try:
        # ── Bronze ────────────────────────────────────────────────────────────
        stage("ingest_orders",    lambda: ingest_orders(
            date_str, config.landing_orders, config.bronze, logger))
        stage("ingest_customers", lambda: ingest_customers(
            config.landing_customers, config.bronze, logger))
        stage("ingest_products",  lambda: ingest_products(
            config.landing_products_db, config.bronze, state, logger))

        # ── Schema fingerprints (post-Bronze) ─────────────────────────────────
        for source, bronze_path in [
            ("orders",    config.bronze / "orders" / f"date={date_str}" / "data.parquet"),
            ("customers", config.bronze / "customers" / "data.parquet"),
            ("products",  config.bronze / "products" / "data.parquet"),
        ]:
            if bronze_path.exists():
                cols = pd.read_parquet(bronze_path).columns.tolist()
                record_fingerprint(source, cols, config.state, logger)

        # ── Silver ────────────────────────────────────────────────────────────
        stage("silver_orders",    lambda: build_silver_orders(
            date_str, config.bronze, config.silver, config.quarantine, logger))
        stage("silver_customers", lambda: build_silver_customers(
            config.bronze, config.silver, config.quarantine, logger, date_str))
        stage("silver_products",  lambda: build_silver_products(
            config.bronze, config.silver, config.quarantine, logger))

        # ── Gold ──────────────────────────────────────────────────────────────
        stage("dim_date",      lambda: build_dim_date(
            config.gold, logger))
        stage("dim_product",   lambda: build_dim_product(
            config.silver, config.gold, logger))
        stage("dim_customer",  lambda: build_dim_customer(
            config.silver, config.gold,
            config.gold_cfg.get("scd2_track_fields", ["city", "country", "email"]),
            logger))
        stage("fact_orders",   lambda: build_fact_orders(
            date_str, config.silver, config.gold, logger))

        # ── Commit pending watermark only on full success ─────────────────────
        state.commit_watermark()
        # ── Gold health check (post-Gold) ─────────────────────────────────────
        stage("gold_health_check", lambda: run_gold_health_check(
            date_str, config.gold, logger))

    except Exception as exc:
        status = "FAIL"
        error_msg = str(exc)
        state.discard_pending_watermark()

    finished_at = datetime.now(timezone.utc)
    metadata = {
        "run_id": run_id,
        "date": date_str,
        "status": status,
        "error": error_msg,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_sec": (finished_at - started_at).total_seconds(),
        "stages": stages,
    }
    state.record_run(metadata)
    log_event(logger, "INFO", "pipeline_end", run_id=run_id,
              **{k: v for k, v in metadata.items() if k not in ("stages", "run_id")})
    return metadata


def inspect_pipeline(date_str: str, config: Config) -> None:
    """Print a human-readable summary of pipeline state for a given date."""
    state = StateManager(config.state)

    print("\n── Watermarks ───────────────────────────────────────────")
    wm = state.get_all_watermarks()
    if wm:
        for k, v in wm.items():
            print(f"  {k}: {v}")
    else:
        print("  (none saved yet)")

    print("\n── Last 5 Runs ──────────────────────────────────────────")
    runs = state.get_recent_runs(5)
    if runs:
        for r in runs:
            stages_summary = ", ".join(
                f"{s['stage']}={'✓' if s['status'] == 'OK' else '✗'}"
                for s in r.get("stages", [])
            )
            print(f"  [{r.get('status')}] {r.get('date')}  run_id={r.get('run_id', 'n/a')}"
                  f"  {r.get('duration_sec', 0):.2f}s")
            if stages_summary:
                print(f"    stages: {stages_summary}")
            if r.get("error"):
                print(f"    error:  {r['error']}")
    else:
        print("  (no runs recorded yet)")

    print("\n── Quarantine Counts ────────────────────────────────────")
    for source in ("orders", "customers", "products"):
        q_dir = config.quarantine / source
        count = len(list(q_dir.glob("*.parquet"))) if q_dir.exists() else 0
        print(f"  {source}: {count} quarantine file(s)")

    print("\n── Output Files for", date_str, "────────────────────────")
    checks = [
        ("bronze/orders",  config.bronze  / "orders"  / f"date={date_str}" / "data.parquet"),
        ("silver/orders",  config.silver  / "orders"  / f"date={date_str}" / "data.parquet"),
        ("gold/fact_orders", config.gold  / "fact_orders" / f"date={date_str}" / "data.parquet"),
        ("gold/dim_product",  config.gold / "dim_product.parquet"),
        ("gold/dim_customer", config.gold / "dim_customer.parquet"),
    ]
    for label, path in checks:
        status = "✓ exists" if path.exists() else "✗ missing"
        print(f"  {label}: {status}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="NovaCart ETL pipeline")
    parser.add_argument("--date",     required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Also process N days before --date")
    parser.add_argument("--config",   default="config/pipeline.yaml")
    parser.add_argument("--inspect",  action="store_true",
                        help="Print pipeline state summary instead of running")
    args = parser.parse_args(argv)

    config = Config.load(args.config)

    if args.inspect:
        inspect_pipeline(args.date, config)
        return 0

    target = datetime.strptime(args.date, "%Y-%m-%d").date()
    dates  = [target - timedelta(days=i) for i in range(args.backfill, -1, -1)]

    failures = 0
    for d in dates:
        result = run_one_date(d.strftime("%Y-%m-%d"), config)
        if result["status"] != "SUCCESS":
            failures += 1
            print(f"[FAIL] {d}: {result['error']}", file=sys.stderr)
        else:
            print(f"[OK]   {d}: {result['duration_sec']:.2f}s  run_id={result['run_id']}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
