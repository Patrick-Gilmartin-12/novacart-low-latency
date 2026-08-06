"""
Cross-platform end-to-end runner. Works on macOS, Linux, and Windows.
Usage: python scripts/run_everything.py

Phase 1 (Nov 7-8): customers.json = v1 (CUST-002 in London)
Phase 2 (Nov 9-10): customers.json swapped to v2 (CUST-002 in Portland)
  → dim_customer ends up with 2 rows for CUST-002 (SCD-2 proof)
  → dim_product shows PROD-001 at $44.99 (SCD-1 proof, watermark picks up update)
"""
from __future__ import annotations
import shutil
import subprocess
import sys
from pathlib import Path

ROOT         = Path(__file__).parent.parent
CUSTOMER_DIR = ROOT / "data" / "landing" / "customers"


def run(cmd: list[str], desc: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {desc}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"\n[FAIL] Step failed: {desc}", file=sys.stderr)
        sys.exit(result.returncode)


def main():
    py = sys.executable

    run([py, "scripts/generate_sample_data.py"],
        "Step 1: Generate sample data")

    # Phase 1: initial customer state (CUST-002 in London)
    run([py, "-m", "src.pipeline", "--date", "2025-11-08", "--backfill", "1"],
        "Step 2a: Run pipeline Nov 7-8 (CUST-002 = London)")

    # Swap to updated customer file (CUST-002 relocated to Portland)
    shutil.copy(CUSTOMER_DIR / "customers_v2.json",
                CUSTOMER_DIR / "customers.json")
    print("\n  [SCD-2 demo] customers.json swapped: CUST-002 now Portland/US")

    # Phase 2: updated customer state — SCD-2 will fire for CUST-002
    run([py, "-m", "src.pipeline", "--date", "2025-11-10", "--backfill", "1"],
        "Step 2b: Run pipeline Nov 9-10 (CUST-002 = Portland -> SCD-2 fires)")

    run([py, "-m", "pytest", "-v"],
        "Step 3: Run test suite")

    print("\nAll steps complete. Check data/ for pipeline output.")


if __name__ == "__main__":
    main()
