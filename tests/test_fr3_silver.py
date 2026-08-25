"""
Dedicated tests for FR-3.1 through FR-3.4 (Silver validation layer).

Tests call build_silver_orders / build_silver_customers directly against
Bronze parquet fixtures — no full pipeline invocation needed.
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.transform.silver import build_silver_orders, build_silver_customers


DATE = "2025-11-07"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _logger():
    import logging
    return logging.getLogger("test_fr3")


def _ts(offset_seconds: int = 0) -> str:
    """Return an ISO-8601 UTC timestamp, optionally offset by N seconds."""
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()


def _write_orders_bronze(bronze_dir: Path, rows: list[dict]) -> Path:
    """Write a minimal Bronze orders parquet (all-string columns + metadata)."""
    out_dir = bronze_dir / "orders" / f"date={DATE}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    # Ensure all business columns are str (Bronze contract)
    for col in ["order_id", "customer_id", "product_id", "order_date",
                "quantity", "unit_price", "status"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def _write_customers_bronze(bronze_dir: Path, rows: list[dict]) -> Path:
    """Write a minimal Bronze customers parquet (address as JSON string + metadata)."""
    import json as _json
    out_dir = bronze_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    records = []
    for r in rows:
        rec = dict(r)
        if isinstance(rec.get("address"), dict):
            rec["address"] = _json.dumps(rec["address"])
        records.append(rec)
    df = pd.DataFrame(records)
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def _good_order(order_id: str = "ORD-001", ingested_at: str | None = None) -> dict:
    return {
        "order_id": order_id,
        "customer_id": "CUST-001",
        "product_id": "PROD-001",
        "order_date": DATE,
        "quantity": "2",
        "unit_price": "49.99",
        "status": "shipped",
        "_source_file": "orders_2025-11-07.csv",
        "_row_hash": "abc123",
        "_ingested_at": ingested_at or _ts(),
    }


def _bad_order(order_id: str = "ORD-BAD") -> dict:
    """Order with quantity=0 — violates Pydantic qty_positive validator."""
    row = _good_order(order_id)
    row["quantity"] = "0"
    return row


def _good_customer(customer_id: str = "CUST-001", ingested_at: str | None = None) -> dict:
    return {
        "customer_id": customer_id,
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "alice@example.com",
        "address": {"city": "NYC", "country": "US"},
        "signup_date": "2024-01-01",
        "tier": "gold",
        "_source_file": "customers.json",
        "_row_hash": "def456",
        "_ingested_at": ingested_at or _ts(),
    }


# ---------------------------------------------------------------------------
# FR-3.1 — Every Bronze row validated against its Pydantic model
# ---------------------------------------------------------------------------

def test_fr3_1_valid_row_passes_pydantic(tmp_path: Path):
    """FR-3.1: A valid Bronze order row survives Pydantic and lands in Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_orders_bronze(bronze, [_good_order()])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "ORD-001"


def test_fr3_1_invalid_row_caught_by_pydantic(tmp_path: Path):
    """FR-3.1: A row violating a Pydantic rule (quantity=0) is absent from Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_orders_bronze(bronze, [_bad_order()])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 0


# ---------------------------------------------------------------------------
# FR-3.2 — Valid rows → Silver parquet
# ---------------------------------------------------------------------------

def test_fr3_2_valid_rows_written_to_silver(tmp_path: Path):
    """FR-3.2: All valid rows are written to the Silver parquet at the expected path."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_orders_bronze(bronze, [_good_order("ORD-001"), _good_order("ORD-002")])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    assert out.exists()
    df = pd.read_parquet(out)
    assert set(df["order_id"]) == {"ORD-001", "ORD-002"}


# ---------------------------------------------------------------------------
# FR-3.3 — Invalid rows → Quarantine parquet with _quarantine_reason column
# ---------------------------------------------------------------------------

def test_fr3_3_invalid_rows_in_quarantine(tmp_path: Path):
    """FR-3.3: An invalid row is written to the quarantine directory."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_orders_bronze(bronze, [_good_order("ORD-001"), _bad_order("ORD-BAD")])
    build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    q_files = list((quarantine / "orders").glob("*.parquet"))
    assert q_files, "Expected at least one quarantine file for orders"
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    assert "ORD-BAD" in q_df["order_id"].values


def test_fr3_3_quarantine_reason_column_present(tmp_path: Path):
    """FR-3.3: Quarantine parquet contains a _quarantine_reason column."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_orders_bronze(bronze, [_bad_order()])
    build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    q_files = list((quarantine / "orders").glob("*.parquet"))
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    assert "_quarantine_reason" in q_df.columns


# ---------------------------------------------------------------------------
# FR-3.4 — Silver deduplicates on primary key (latest _ingested_at wins)
# ---------------------------------------------------------------------------

def test_fr3_4_latest_ingested_at_wins(tmp_path: Path):
    """FR-3.4: When two customer rows share a customer_id, the one with the
    later _ingested_at survives in Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    earlier = _ts(offset_seconds=-60)
    later   = _ts(offset_seconds=0)

    old_row = _good_customer("CUST-001", ingested_at=earlier)
    old_row["first_name"] = "OldAlice"

    new_row = _good_customer("CUST-001", ingested_at=later)
    new_row["first_name"] = "NewAlice"

    _write_customers_bronze(bronze, [old_row, new_row])
    out = build_silver_customers(bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df.iloc[0]["first_name"] == "NewAlice"


def test_fr3_4_earlier_ingested_at_absent(tmp_path: Path):
    """FR-3.4: The earlier-timestamped duplicate row does not appear in Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    earlier = _ts(offset_seconds=-60)
    later   = _ts(offset_seconds=0)

    old_row = _good_customer("CUST-001", ingested_at=earlier)
    old_row["first_name"] = "OldAlice"

    new_row = _good_customer("CUST-001", ingested_at=later)
    new_row["first_name"] = "NewAlice"

    _write_customers_bronze(bronze, [old_row, new_row])
    out = build_silver_customers(bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert "OldAlice" not in df["first_name"].values
