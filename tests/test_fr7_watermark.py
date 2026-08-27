"""
Tests for FR-7.1 and FR-7.2: Products watermark stored in state/watermarks.json
and only advanced after a successful Bronze write.

Calls ingest_products directly against a real SQLite fixture so every
scenario exercises the full read → filter → write → advance chain.
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from tests.conftest import make_products_db
from src.ingest.products import ingest_products, WATERMARK_KEY
from src.utils.state import StateManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _logger():
    import logging
    return logging.getLogger("test_fr7")


def _state(state_dir: Path) -> StateManager:
    return StateManager(state_dir)


def _read_watermark(state_dir: Path) -> str | None:
    """Read the raw watermark value from disk, or None if the file doesn't exist."""
    wm_file = state_dir / "watermarks.json"
    if not wm_file.exists():
        return None
    return json.loads(wm_file.read_text()).get(WATERMARK_KEY)


# ---------------------------------------------------------------------------
# FR-7.1a — First run (no watermarks.json): all products loaded, watermark written
# ---------------------------------------------------------------------------

def test_first_run_loads_all_products(tmp_path: Path):
    """With no prior watermarks.json every product in the DB is ingested."""
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    make_products_db(db, [
        ("P-001", "Widget",  "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
        ("P-002", "Gadget",  "Electronics", 20.0, "SUP-A", "2025-01-02T09:00:00"),
        ("P-003", "Doohickey","Accessories", 5.0, "SUP-B", "2025-01-03T09:00:00"),
    ])

    ingest_products(db, bronze, _state(state_dir), _logger())

    df = pd.read_parquet(bronze / "products" / "data.parquet")
    assert set(df["product_id"]) == {"P-001", "P-002", "P-003"}


def test_first_run_writes_watermark_file(tmp_path: Path):
    """After the first run, state/watermarks.json must exist and contain the key."""
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
    ])

    assert _read_watermark(state_dir) is None  # nothing on disk before run

    ingest_products(db, bronze, _state(state_dir), _logger())

    assert _read_watermark(state_dir) is not None, (
        "watermarks.json should exist after the first successful ingest"
    )


def test_first_run_watermark_equals_max_updated_at(tmp_path: Path):
    """The written watermark must equal the maximum updated_at value in the batch."""
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
        ("P-002", "Gadget", "Electronics", 20.0, "SUP-A", "2025-06-15T12:00:00"),  # latest
    ])

    ingest_products(db, bronze, _state(state_dir), _logger())

    assert _read_watermark(state_dir) == "2025-06-15T12:00:00"


# ---------------------------------------------------------------------------
# FR-7.1b — Second run: only rows newer than the watermark are loaded
# ---------------------------------------------------------------------------

def test_second_run_skips_already_ingested_rows(tmp_path: Path):
    """A second run must not re-ingest products whose updated_at <= the watermark."""
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    # Seed the DB with two products
    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
        ("P-002", "Gadget", "Electronics", 20.0, "SUP-A", "2025-01-02T09:00:00"),
    ])

    # First run ingests both; watermark advances to 2025-01-02T09:00:00
    ingest_products(db, bronze, _state(state_dir), _logger())

    # Add one new product after the watermark
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO products VALUES (?,?,?,?,?,?)",
        ("P-003", "Doohickey", "Accessories", 5.0, "SUP-B", "2025-03-01T08:00:00"),
    )
    conn.commit()
    conn.close()

    # Second run should only pick up P-003
    ingest_products(db, bronze, _state(state_dir), _logger())

    df = pd.read_parquet(bronze / "products" / "data.parquet")
    assert list(df["product_id"]) == ["P-003"], (
        f"Expected only P-003 on the second run, got {list(df['product_id'])}"
    )


def test_second_run_advances_watermark(tmp_path: Path):
    """After a second run the watermark must advance to the new batch's max updated_at."""
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
    ])
    ingest_products(db, bronze, _state(state_dir), _logger())
    assert _read_watermark(state_dir) == "2025-01-01T09:00:00"

    # Add a newer product
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO products VALUES (?,?,?,?,?,?)",
        ("P-002", "Gadget", "Electronics", 20.0, "SUP-A", "2025-06-01T00:00:00"),
    )
    conn.commit()
    conn.close()

    ingest_products(db, bronze, _state(state_dir), _logger())
    assert _read_watermark(state_dir) == "2025-06-01T00:00:00"


# ---------------------------------------------------------------------------
# FR-7.1c — Empty result: watermark is unchanged when no new rows exist
# ---------------------------------------------------------------------------

def test_empty_result_does_not_move_watermark(tmp_path: Path):
    """If no rows are newer than the watermark, the watermark value must not change."""
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
    ])

    # First run sets the watermark
    ingest_products(db, bronze, _state(state_dir), _logger())
    watermark_after_first = _read_watermark(state_dir)

    # Second run — no new products added, DB unchanged
    ingest_products(db, bronze, _state(state_dir), _logger())
    watermark_after_second = _read_watermark(state_dir)

    assert watermark_after_first == watermark_after_second, (
        "Watermark must not change when no new rows are ingested"
    )


# ---------------------------------------------------------------------------
# FR-7.2 — Watermark advances ONLY after successful Bronze write (never before)
# ---------------------------------------------------------------------------

def test_watermark_not_advanced_when_write_fails(tmp_path: Path):
    """FR-7.2: If the Bronze parquet write raises, the watermark must be unchanged.

    Simulates an I/O failure by patching DataFrame.to_parquet to raise an
    OSError, then confirms the watermark on disk is still None (no prior run
    had set it), proving set_watermark was never called.
    """
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
    ])

    with patch("pandas.DataFrame.to_parquet", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            ingest_products(db, bronze, _state(state_dir), _logger())

    assert _read_watermark(state_dir) is None, (
        "Watermark must not be written when the Bronze parquet write fails"
    )


def test_watermark_not_advanced_when_write_fails_with_prior_watermark(tmp_path: Path):
    """FR-7.2: A failed write must leave the watermark at its previous value,
    not advance it to the new batch — so the next run retries the same rows.
    """
    db = tmp_path / "products.db"
    bronze = tmp_path / "bronze"
    state_dir = tmp_path / "state"

    # First run succeeds — watermark is set to the first product's updated_at
    make_products_db(db, [
        ("P-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T09:00:00"),
    ])
    ingest_products(db, bronze, _state(state_dir), _logger())
    watermark_before = _read_watermark(state_dir)
    assert watermark_before == "2025-01-01T09:00:00"

    # Add a newer product that the second run would pick up
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO products VALUES (?,?,?,?,?,?)",
        ("P-002", "Gadget", "Electronics", 20.0, "SUP-A", "2025-06-01T00:00:00"),
    )
    conn.commit()
    conn.close()

    # Second run fails at the parquet write step
    with patch("pandas.DataFrame.to_parquet", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            ingest_products(db, bronze, _state(state_dir), _logger())

    # Watermark must still be the value from the first successful run
    assert _read_watermark(state_dir) == watermark_before, (
        "Watermark must not advance past the last successful write"
    )
