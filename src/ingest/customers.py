"""Ingest customers nested JSON → Bronze parquet."""
from __future__ import annotations
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "customer_id", "first_name", "last_name", "email",
    "address", "signup_date", "tier",
]


def _row_hash(row: pd.Series, cols: list) -> str:
    """SHA-256 of pipe-joined string values of business columns (deterministic order)."""
    joined = "|".join(str(row[c]) for c in cols)
    return hashlib.sha256(joined.encode()).hexdigest()


def ingest_customers(
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
    run_id: str = "",
) -> Path:
    """Read nested JSON export and write to Bronze. Returns output path."""
    src = landing_dir / "customers.json"
    if not src.exists():
        raise IngestionError(f"customers file not found: {src}")

    raw = json.loads(src.read_text())

    # FR-1.2: preserve nested address as JSON string; do NOT flatten at Bronze
    rows = []
    for rec in raw:
        rec["address"] = json.dumps(rec.get("address", {}))
        rows.append(rec)

    df = pd.DataFrame(rows)
    rows_in = len(df)
    log_event(logger, "INFO", "customers_ingested", rows=rows_in, run_id=run_id)

    check_schema(list(df.columns), EXPECTED_COLUMNS, "customers", logger)

    # FR-1.5: cast all business columns to string (no type inference at Bronze)
    df = df.astype(str)

    # FR-1.4: metadata columns
    df["_row_hash"] = df.apply(lambda row: _row_hash(row, EXPECTED_COLUMNS), axis=1)
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = bronze_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "customers_bronze_written", path=str(out_path),
              stage="bronze", rows_in=rows_in, rows_out=len(df), rows_quarantined=0,
              run_id=run_id)
    return out_path
