"""
Dedicated tests for FR-4.1 through FR-4.4 (Gold layer — star schema).

Tests call build_dim_date / build_dim_product / build_dim_customer /
build_fact_orders directly against Silver parquet fixtures — no full
pipeline invocation needed.
"""
from __future__ import annotations
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.transform.gold import (
    build_dim_date,
    build_dim_product,
    build_dim_customer,
    build_fact_orders,
)


DATE = "2025-11-08"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _logger():
    import logging
    return logging.getLogger("test_fr4")


def _write_silver_orders(silver_dir: Path, date_str: str, rows: list[dict]) -> Path:
    """Write a minimal Silver orders parquet (no _-prefixed columns)."""
    out_dir = silver_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def _write_silver_customers(silver_dir: Path, rows: list[dict]) -> Path:
    """Write a minimal Silver customers parquet (no _-prefixed columns)."""
    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def _write_silver_products(silver_dir: Path, rows: list[dict]) -> Path:
    """Write a minimal Silver products parquet (no _-prefixed columns)."""
    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def _good_product(
    product_id: str = "PROD-001",
    name: str = "Widget A",
    unit_cost: float = 10.0,
) -> dict:
    return {
        "product_id": product_id,
        "name": name,
        "category": "Widgets",
        "unit_cost": unit_cost,
        "supplier_id": "SUP-001",
        "updated_at": "2025-01-01T00:00:00",
    }


def _good_customer(
    customer_id: str = "CUST-001",
    city: str = "NYC",
    country: str = "US",
    email: str = "alice@example.com",
) -> dict:
    return {
        "customer_id": customer_id,
        "first_name": "Alice",
        "last_name": "Smith",
        "email": email,
        "city": city,
        "country": country,
        "signup_date": "2024-01-01",
        "tier": "gold",
    }


def _good_order(
    order_id: str = "ORD-001",
    quantity: int = 2,
    unit_price: float = 49.99,
    date_str: str = DATE,
) -> dict:
    return {
        "order_id": order_id,
        "customer_id": "CUST-001",
        "product_id": "PROD-001",
        "order_date": date_str,
        "quantity": quantity,
        "unit_price": unit_price,
        "status": "shipped",
    }


# ---------------------------------------------------------------------------
# FR-4.1 — dim_date calendar dimension
# ---------------------------------------------------------------------------

def test_fr4_1_dim_date_row_count(tmp_path: Path):
    """FR-4.1: dim_date covers 2024-01-01 → 2026-12-31 inclusive (1096 rows)."""
    gold = tmp_path / "gold"
    out = build_dim_date(gold, _logger())
    df = pd.read_parquet(out)
    assert len(df) == 1096


def test_fr4_1_dim_date_columns_present(tmp_path: Path):
    """FR-4.1: All 10 required columns are present in dim_date."""
    gold = tmp_path / "gold"
    out = build_dim_date(gold, _logger())
    df = pd.read_parquet(out)
    expected = {
        "date_key", "year", "month", "day", "quarter",
        "week", "day_of_week", "day_name", "month_name", "is_weekend",
    }
    assert expected.issubset(set(df.columns))


def test_fr4_1_dim_date_key_is_date_type(tmp_path: Path):
    """FR-4.1: date_key values are plain datetime.date objects (not Timestamps)."""
    gold = tmp_path / "gold"
    out = build_dim_date(gold, _logger())
    df = pd.read_parquet(out)
    assert isinstance(df["date_key"].iloc[0], date)


def test_fr4_1_dim_date_boundary_dates(tmp_path: Path):
    """FR-4.1: Both boundary dates (2024-01-01 and 2026-12-31) are present."""
    gold = tmp_path / "gold"
    out = build_dim_date(gold, _logger())
    df = pd.read_parquet(out)
    values = set(df["date_key"].values)
    assert date(2024, 1, 1) in values
    assert date(2026, 12, 31) in values


# ---------------------------------------------------------------------------
# FR-4.2 — dim_product SCD Type 1 (overwrite)
# ---------------------------------------------------------------------------

def test_fr4_2_scd1_initial_load(tmp_path: Path):
    """FR-4.2: Initial load writes all products to dim_product.parquet."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_products(silver, [_good_product("PROD-001"), _good_product("PROD-002")])
    out = build_dim_product(silver, gold, _logger())

    df = pd.read_parquet(out)
    assert len(df) == 2


def test_fr4_2_scd1_overwrite_on_collision(tmp_path: Path):
    """FR-4.2: Re-run with updated unit_cost overwrites old value; one row per product_id."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    # First load
    _write_silver_products(silver, [_good_product("PROD-001", unit_cost=10.0)])
    build_dim_product(silver, gold, _logger())

    # Second load — same product_id, new unit_cost
    _write_silver_products(silver, [_good_product("PROD-001", unit_cost=99.0)])
    out = build_dim_product(silver, gold, _logger())

    df = pd.read_parquet(out)
    prod_rows = df[df["product_id"] == "PROD-001"]
    assert len(prod_rows) == 1
    assert prod_rows.iloc[0]["unit_cost"] == 99.0


# ---------------------------------------------------------------------------
# FR-4.3 — dim_customer SCD Type 2 (track history)
# ---------------------------------------------------------------------------

SCD2_FIELDS = ["city", "country", "email"]


def test_fr4_3_scd2_initial_load(tmp_path: Path):
    """FR-4.3: Initial load sets _current=True and _eff_end='9999-12-31' for all rows."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_customers(silver, [_good_customer("CUST-001"), _good_customer("CUST-002")])
    out = build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    df = pd.read_parquet(out)
    assert all(df["_current"] == True)
    assert all(df["_eff_end"] == "9999-12-31")


def test_fr4_3_scd2_unchanged_customer_no_new_row(tmp_path: Path):
    """FR-4.3: Re-running with identical silver data produces only one row per customer."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_customers(silver, [_good_customer("CUST-001")])
    build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    # Second run — same data
    _write_silver_customers(silver, [_good_customer("CUST-001")])
    out = build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    df = pd.read_parquet(out)
    assert len(df[df["customer_id"] == "CUST-001"]) == 1


def test_fr4_3_scd2_tracked_field_change_closes_old_row(tmp_path: Path):
    """FR-4.3: A tracked-field change expires the old row and opens a new current row."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    # First load — city=NYC
    _write_silver_customers(silver, [_good_customer("CUST-001", city="NYC")])
    build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    # Second load — city changed to LA
    _write_silver_customers(silver, [_good_customer("CUST-001", city="LA")])
    out = build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    df = pd.read_parquet(out)
    cust_rows = df[df["customer_id"] == "CUST-001"]

    old_row = cust_rows[cust_rows["city"] == "NYC"]
    assert len(old_row) == 1
    assert old_row.iloc[0]["_current"] == False

    new_row = cust_rows[cust_rows["city"] == "LA"]
    assert len(new_row) == 1
    assert new_row.iloc[0]["_current"] == True


def test_fr4_3_scd2_new_customer_inserted(tmp_path: Path):
    """FR-4.3: A customer absent from the first load appears as _current=True on second load."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    # First load — only CUST-001
    _write_silver_customers(silver, [_good_customer("CUST-001")])
    build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    # Second load — CUST-001 + CUST-002
    _write_silver_customers(silver, [_good_customer("CUST-001"), _good_customer("CUST-002")])
    out = build_dim_customer(silver, gold, SCD2_FIELDS, _logger())

    df = pd.read_parquet(out)
    new_cust = df[df["customer_id"] == "CUST-002"]
    assert len(new_cust) == 1
    assert new_cust.iloc[0]["_current"] == True


# ---------------------------------------------------------------------------
# FR-4.4 — fact_orders idempotent partition-replace
# ---------------------------------------------------------------------------

def test_fr4_4_fact_orders_written_at_correct_path(tmp_path: Path):
    """FR-4.4: build_fact_orders returns a path that exists at the expected location."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_orders(silver, DATE, [_good_order()])
    out = build_fact_orders(DATE, silver, gold, _logger())

    assert out.exists()
    assert out == gold / "fact_orders" / f"date={DATE}" / "data.parquet"


def test_fr4_4_fact_orders_idempotent(tmp_path: Path):
    """FR-4.4: Calling build_fact_orders twice for the same date produces the same row count."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_orders(silver, DATE, [_good_order("ORD-001"), _good_order("ORD-002")])
    out = build_fact_orders(DATE, silver, gold, _logger())
    count_first = len(pd.read_parquet(out))

    out = build_fact_orders(DATE, silver, gold, _logger())
    count_second = len(pd.read_parquet(out))

    assert count_first == count_second


def test_fr4_4_order_date_is_plain_date(tmp_path: Path):
    """FR-4.4: order_date in fact_orders is a plain datetime.date (not a Timestamp)."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_orders(silver, DATE, [_good_order()])
    out = build_fact_orders(DATE, silver, gold, _logger())

    df = pd.read_parquet(out)
    assert isinstance(df["order_date"].iloc[0], date)


def test_fr4_4_total_amount_dtype_is_float64(tmp_path: Path):
    """FR-4.4: total_amount column dtype is float64."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_orders(silver, DATE, [_good_order()])
    out = build_fact_orders(DATE, silver, gold, _logger())

    df = pd.read_parquet(out)
    assert df["total_amount"].dtype == "float64"


def test_fr4_4_total_amount_value_correct(tmp_path: Path):
    """FR-4.4: total_amount equals quantity * unit_price for a known order."""
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    _write_silver_orders(silver, DATE, [_good_order(order_id="ORD-001", quantity=3, unit_price=10.0)])
    out = build_fact_orders(DATE, silver, gold, _logger())

    df = pd.read_parquet(out)
    row = df[df["order_id"] == "ORD-001"].iloc[0]
    assert row["total_amount"] == 30.0
