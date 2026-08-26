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


# ---------------------------------------------------------------------------
# FK check — product_id validation against Silver products catalogue
# ---------------------------------------------------------------------------

def _write_silver_products(silver_dir: Path, product_ids: list[str]) -> Path:
    """Write a minimal Silver products parquet containing the given product IDs."""
    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "product_id": product_ids,
        "name": ["Product"] * len(product_ids),
        "category": ["Cat"] * len(product_ids),
        "unit_cost": [1.0] * len(product_ids),
        "supplier_id": ["SUP-A"] * len(product_ids),
        "updated_at": ["2025-01-01T00:00:00"] * len(product_ids),
    })
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def test_fk_valid_product_id_passes(tmp_path: Path):
    """An order whose product_id exists in the Silver products catalogue reaches Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_products(silver, ["PROD-001"])
    _write_orders_bronze(bronze, [_good_order("ORD-001")])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "ORD-001"


def test_fk_invalid_product_id_quarantined(tmp_path: Path):
    """An order whose product_id is not in the Silver products catalogue is quarantined."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_products(silver, ["PROD-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["product_id"] = "PROD-UNKNOWN"
    _write_orders_bronze(bronze, [bad_order])
    build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    q_files = list((quarantine / "orders").glob("*.parquet"))
    assert q_files, "Expected a quarantine file for the FK-violating order"
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    assert "ORD-BAD" in q_df["order_id"].values


def test_fk_invalid_product_id_absent_from_silver(tmp_path: Path):
    """An order with an unknown product_id must not appear in Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_products(silver, ["PROD-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["product_id"] = "PROD-UNKNOWN"
    _write_orders_bronze(bronze, [bad_order])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 0


def test_fk_quarantine_reason_describes_violation(tmp_path: Path):
    """The quarantine record for an FK violation includes the offending product_id in the reason."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_products(silver, ["PROD-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["product_id"] = "PROD-UNKNOWN"
    _write_orders_bronze(bronze, [bad_order])
    build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    q_files = list((quarantine / "orders").glob("*.parquet"))
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    reasons = q_df["_quarantine_reason"].tolist()
    assert any("PROD-UNKNOWN" in r for r in reasons), (
        f"Expected 'PROD-UNKNOWN' in quarantine reason, got: {reasons}"
    )


def test_fk_mixed_orders_only_valid_reach_silver(tmp_path: Path):
    """When one order is valid and one has an unknown product_id, only the valid one reaches Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_products(silver, ["PROD-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["product_id"] = "PROD-UNKNOWN"
    _write_orders_bronze(bronze, [_good_order("ORD-GOOD"), bad_order])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "ORD-GOOD"


def test_fk_no_products_catalogue_passes_all_orders(tmp_path: Path):
    """When the Silver products parquet does not exist, FK enforcement is skipped
    and all otherwise-valid orders pass through to Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    # Deliberately do NOT write a silver products parquet
    _write_orders_bronze(bronze, [_good_order("ORD-001")])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1


# ---------------------------------------------------------------------------
# FK check — customer_id validation against Silver customers catalogue
# ---------------------------------------------------------------------------

def _write_silver_customers(silver_dir: Path, customer_ids: list[str]) -> Path:
    """Write a minimal Silver customers parquet containing the given customer IDs."""
    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "first_name": ["Test"] * len(customer_ids),
        "last_name": ["User"] * len(customer_ids),
        "email": [f"user{i}@example.com" for i in range(len(customer_ids))],
        "city": ["NYC"] * len(customer_ids),
        "country": ["US"] * len(customer_ids),
        "signup_date": ["2024-01-01"] * len(customer_ids),
        "tier": ["standard"] * len(customer_ids),
    })
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def test_customer_fk_valid_customer_id_passes(tmp_path: Path):
    """An order whose customer_id exists in the Silver customers catalogue reaches Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_customers(silver, ["CUST-001"])
    _write_orders_bronze(bronze, [_good_order("ORD-001")])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "ORD-001"


def test_customer_fk_invalid_customer_id_quarantined(tmp_path: Path):
    """An order whose customer_id is not in the Silver customers catalogue is quarantined."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_customers(silver, ["CUST-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["customer_id"] = "CUST-UNKNOWN"
    _write_orders_bronze(bronze, [bad_order])
    build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    q_files = list((quarantine / "orders").glob("*.parquet"))
    assert q_files, "Expected a quarantine file for the customer FK-violating order"
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    assert "ORD-BAD" in q_df["order_id"].values


def test_customer_fk_invalid_customer_id_absent_from_silver(tmp_path: Path):
    """An order with an unknown customer_id must not appear in Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_customers(silver, ["CUST-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["customer_id"] = "CUST-UNKNOWN"
    _write_orders_bronze(bronze, [bad_order])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 0


def test_customer_fk_quarantine_reason_describes_violation(tmp_path: Path):
    """The quarantine record for a customer FK violation includes the offending customer_id."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_customers(silver, ["CUST-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["customer_id"] = "CUST-UNKNOWN"
    _write_orders_bronze(bronze, [bad_order])
    build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    q_files = list((quarantine / "orders").glob("*.parquet"))
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    reasons = q_df["_quarantine_reason"].tolist()
    assert any("CUST-UNKNOWN" in r for r in reasons), (
        f"Expected 'CUST-UNKNOWN' in quarantine reason, got: {reasons}"
    )


def test_customer_fk_mixed_orders_only_valid_reach_silver(tmp_path: Path):
    """When one order has a valid customer_id and one does not, only the valid one reaches Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    _write_silver_customers(silver, ["CUST-001"])
    bad_order = _good_order("ORD-BAD")
    bad_order["customer_id"] = "CUST-UNKNOWN"
    _write_orders_bronze(bronze, [_good_order("ORD-GOOD"), bad_order])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "ORD-GOOD"


def test_customer_fk_no_customers_catalogue_passes_all_orders(tmp_path: Path):
    """When the Silver customers parquet does not exist, FK enforcement is skipped
    and all otherwise-valid orders pass through to Silver."""
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"

    # Deliberately do NOT write a silver customers parquet
    _write_orders_bronze(bronze, [_good_order("ORD-001")])
    out = build_silver_orders(DATE, bronze, silver, quarantine, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 1
