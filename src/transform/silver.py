"""Bronze → Silver: validate with Pydantic, dedupe, quarantine bad rows."""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Type

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.utils.logging_setup import log_event
from src.utils.schemas import OrderRow, CustomerRow, ProductRow
from src.utils.state import StateManager


def _strip_meta(df: pd.DataFrame) -> pd.DataFrame:
    """Drop all Bronze metadata columns (those starting with '_')."""
    meta_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(columns=meta_cols)


def _dedup_by_ingested_at(
    df: pd.DataFrame,
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
    state: StateManager | None = None,
) -> pd.DataFrame:
    """Deduplicate on primary_key keeping the row with the latest _ingested_at.

    Requires _ingested_at to be present in df. If the column is absent the
    DataFrame is returned unchanged (safe fallback).

    Superseded duplicate rows are written to quarantine with
    _quarantine_reason = "duplicate: superseded by later _ingested_at".
    Rows whose _row_hash was already quarantined in a previous run are skipped
    (not re-written) to avoid duplicate quarantine files.
    """
    if "_ingested_at" not in df.columns or primary_key not in df.columns or df.empty:
        return df

    df_sorted = df.sort_values("_ingested_at", ascending=True)
    deduped = df_sorted.drop_duplicates(subset=[primary_key], keep="last").reset_index(drop=True)
    dupes = len(df) - len(deduped)

    if dupes:
        duplicate_rows = df_sorted[
            df_sorted.index.isin(
                df_sorted.index.difference(
                    df_sorted.drop_duplicates(subset=[primary_key], keep="last").index
                )
            )
        ].copy()
        df_stripped = _strip_meta(duplicate_rows)

        seen_hashes = state.get_quarantined_hashes(source_name) if state else set()
        bad, new_hashes = [], set()
        for idx in duplicate_rows.index:
            row_dict = df_stripped.loc[idx].to_dict()
            row_hash = duplicate_rows.loc[idx].get("_row_hash", "")
            if row_hash and row_hash in seen_hashes:
                continue
            row_dict["_quarantine_reason"] = "duplicate: superseded by later _ingested_at"
            row_dict["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            bad.append(row_dict)
            if row_hash:
                new_hashes.add(row_hash)

        if bad:
            q_dir = quarantine_path / source_name
            q_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            pd.DataFrame(bad).to_parquet(q_dir / f"{ts}.parquet", index=False)
            if state and new_hashes:
                state.add_quarantined_hashes(source_name, new_hashes)
            log_event(logger, "WARNING", f"{source_name}_duplicates_quarantined", count=len(bad))

        log_event(logger, "INFO", f"{source_name}_deduped", dropped=dupes)

    return deduped


def _validate_df(
    df: pd.DataFrame,
    model: Type[BaseModel],
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
    state: StateManager | None = None,
) -> pd.DataFrame:
    """Validate each row with Pydantic. Good rows (with Bronze metadata) → returned.
    Bad rows → quarantine parquet.

    Accepts the full Bronze DataFrame (metadata columns included). Strips
    metadata internally for Pydantic validation but returns good rows with
    metadata still attached so callers can deduplicate by _ingested_at before
    stripping.

    Rows whose _row_hash was already quarantined in a previous run are skipped
    (not re-written) to avoid duplicate quarantine files across pipeline runs.
    """
    df_stripped = _strip_meta(df)
    seen_hashes = state.get_quarantined_hashes(source_name) if state else set()
    good, bad, new_hashes = [], [], set()
    for idx, row in df.iterrows():
        stripped_row = df_stripped.loc[idx]
        try:
            model(**stripped_row.to_dict())
            good.append(row)
        except (ValidationError, Exception) as exc:
            row_hash = row.get("_row_hash", "")
            if row_hash and row_hash in seen_hashes:
                # Already quarantined in a prior run — skip re-writing
                continue
            row_dict = stripped_row.to_dict()
            row_dict["_quarantine_reason"] = str(exc)
            row_dict["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            bad.append(row_dict)
            if row_hash:
                new_hashes.add(row_hash)

    if bad:
        q_dir = quarantine_path / source_name
        q_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        pd.DataFrame(bad).to_parquet(q_dir / f"{ts}.parquet", index=False)
        if state and new_hashes:
            state.add_quarantined_hashes(source_name, new_hashes)
        log_event(logger, "WARNING", f"{source_name}_quarantined", count=len(bad))

    # Return good rows with metadata intact — callers dedup then strip.
    return pd.DataFrame(good).reset_index(drop=True) if good else pd.DataFrame(columns=df.columns)


def build_silver_orders(
    date_str: str,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
    state: StateManager | None = None,
) -> Path:
    src = bronze_dir / "orders" / f"date={date_str}" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_orders_no_bronze", date=date_str)
        return silver_dir / "orders" / f"date={date_str}" / "data.parquet"

    df = pd.read_parquet(src)

    # FR-1.5 compensation: cast string columns back to their target types
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df = _validate_df(df, OrderRow, "order_id", quarantine_dir, logger, "orders", state)
    df = _dedup_by_ingested_at(df, "order_id", quarantine_dir, logger, "orders", state)
    df = _strip_meta(df)

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
    state: StateManager | None = None,
) -> Path:
    src = bronze_dir / "customers" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_customers_no_bronze")
        return silver_dir / "customers" / "data.parquet"

    df = pd.read_parquet(src)

    # FR-1.2 compensation: expand address JSON string → flat city/country columns
    def _expand_address(row: pd.Series) -> pd.Series:
        try:
            addr = json.loads(row["address"])
        except (ValueError, KeyError, TypeError):
            addr = {}
        row["city"] = addr.get("city", "")
        row["country"] = addr.get("country", "")
        return row

    df = df.apply(_expand_address, axis=1).drop(columns=["address"])

    df = _validate_df(df, CustomerRow, "customer_id", quarantine_dir, logger, "customers", state)
    df = _dedup_by_ingested_at(df, "customer_id", quarantine_dir, logger, "customers", state)
    df = _strip_meta(df)

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
    state: StateManager | None = None,
) -> Path:
    src = bronze_dir / "products" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_products_no_bronze")
        return silver_dir / "products" / "data.parquet"

    df = pd.read_parquet(src)
    if df.empty:
        return silver_dir / "products" / "data.parquet"

    # FR-1.5 compensation: cast string columns back to their target types
    df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce")

    df = _validate_df(df, ProductRow, "product_id", quarantine_dir, logger, "products", state)
    df = _dedup_by_ingested_at(df, "product_id", quarantine_dir, logger, "products", state)
    df = _strip_meta(df)

    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_products_written", rows=len(df))
    return out_path
