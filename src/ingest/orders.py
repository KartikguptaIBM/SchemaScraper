"""Ingest daily orders CSV → Bronze parquet."""
from __future__ import annotations
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "order_id", "customer_id", "product_id",
    "order_date", "quantity", "unit_price", "status",
]


def _row_hash(row: pd.Series, cols: list) -> str:
    """SHA-256 of pipe-joined string values of business columns (deterministic order)."""
    joined = "|".join(str(row[c]) for c in cols)
    return hashlib.sha256(joined.encode()).hexdigest()


def _read_csv_with_fallback(src: Path, logger: logging.Logger) -> pd.DataFrame:
    """Try UTF-8 first; fall back to Latin-1 on decode error."""
    try:
        return pd.read_csv(src, encoding="utf-8")
    except UnicodeDecodeError:
        log_event(logger, "INFO", "orders_encoding_fallback", file=src.name)
        try:
            return pd.read_csv(src, encoding="latin-1")
        except Exception as exc:
            raise IngestionError(f"cannot decode {src}: {exc}") from exc


def ingest_orders(
    date_str: str,
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Read orders_YYYY-MM-DD.csv and write to Bronze layer. Returns output path."""
    src = landing_dir / f"orders_{date_str}.csv"
    if not src.exists():
        raise IngestionError(f"orders file not found: {src}")

    df = _read_csv_with_fallback(src, logger)
    log_event(logger, "INFO", "orders_ingested", date=date_str, rows=len(df))

    check_schema(list(df.columns), EXPECTED_COLUMNS, "orders", logger)

    # FR-1.5: cast all business columns to string (no type inference at Bronze)
    df = df.astype(str)

    # FR-1.4: metadata columns
    df["_row_hash"] = df.apply(lambda row: _row_hash(row, EXPECTED_COLUMNS), axis=1)
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_partition_date"] = date_str

    out_dir = bronze_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "orders_bronze_written", path=str(out_path), rows=len(df))
    return out_path
