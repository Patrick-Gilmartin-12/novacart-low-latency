"""Schema fingerprint storage.

After Bronze ingest, hashes the sorted column list of each Bronze parquet
into a SHA-256 fingerprint and persists it to state/fingerprints.json.

If the fingerprint for a source changes (columns added or removed), a
schema_fingerprint_changed WARNING is logged. The new fingerprint is always
stored so the next run has an up-to-date baseline.
"""
from __future__ import annotations
import hashlib
import json
import logging
from pathlib import Path

from src.utils.logging_setup import log_event


def _fingerprint(columns: list[str]) -> str:
    """SHA-256 of the sorted, pipe-joined column names."""
    normalised = "|".join(sorted(columns))
    return hashlib.sha256(normalised.encode()).hexdigest()


def _load(fp_file: Path) -> dict:
    if fp_file.exists():
        return json.loads(fp_file.read_text())
    return {}


def _save(fp_file: Path, data: dict) -> None:
    fp_file.write_text(json.dumps(data, indent=2))


def record_fingerprint(
    source: str,
    columns: list[str],
    state_dir: Path,
    logger: logging.Logger,
) -> None:
    """Compute and store a schema fingerprint for *source*.

    Parameters
    ----------
    source:
        Short name for the data source, e.g. ``"orders"``, ``"customers"``.
    columns:
        Column list of the freshly ingested Bronze parquet.
    state_dir:
        Pipeline state directory (config.state).
    logger:
        Pipeline logger.
    """
    fp_file = state_dir / "fingerprints.json"
    stored = _load(fp_file)
    current = _fingerprint(columns)
    previous = stored.get(source)

    if previous is None:
        log_event(
            logger, "INFO", "schema_fingerprint_new",
            source=source, fingerprint=current, columns=sorted(columns),
        )
    elif previous != current:
        log_event(
            logger, "WARNING", "schema_fingerprint_changed",
            source=source,
            previous=previous,
            current=current,
            columns=sorted(columns),
        )
    else:
        log_event(
            logger, "INFO", "schema_fingerprint_unchanged",
            source=source, fingerprint=current,
        )

    stored[source] = current
    _save(fp_file, stored)
