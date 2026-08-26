"""
FR-5 Orchestration tests.

Covers all three FR-5 acceptance criteria:
  FR-5.1  CLI entry-point returns correct exit codes.
  FR-5.2  --backfill N processes exactly N+1 dates in chronological order.
  FR-5.3  Every run appends a well-formed metadata record to state/runs.jsonl.

Each test is self-contained: it seeds its own input data via the shared
conftest helpers and relies on the isolated `config` fixture (temp directory).
Adding more sources or stages in future only requires updating the seed helpers
already used here — the assertions remain stable.
"""
from __future__ import annotations
import csv
import json
from pathlib import Path

import pytest

from src.pipeline import main, run_one_date
from src.utils.config import Config
from tests.conftest import make_products_db, write_customers_json, write_orders_csv

# ---------------------------------------------------------------------------
# Shared seed data — kept at module level so every test can reuse them without
# re-declaring.  Add new sources here if the pipeline grows.
# ---------------------------------------------------------------------------
DATE_A = "2025-11-07"
DATE_B = "2025-11-08"
DATE_C = "2025-11-09"

GOOD_CUSTOMER = {
    "customer_id": "CUST-001",
    "first_name": "Alice",
    "last_name": "Smith",
    "email": "alice@example.com",
    "address": {"city": "NYC", "country": "US"},
    "signup_date": "2024-01-01",
    "tier": "gold",
}
GOOD_PRODUCT = ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00")


def _seed_date(config: Config, date_str: str) -> None:
    """Write the minimum valid input for one pipeline date."""
    write_orders_csv(
        config.landing_orders,
        date_str,
        [[f"ORD-{date_str[-2:]}", "CUST-001", "PROD-001", date_str, "1", "9.99", "shipped"]],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])


def _read_runs(config: Config) -> list[dict]:
    """Parse state/runs.jsonl into a list of run records."""
    runs_file: Path = config.state / "runs.jsonl"
    return [json.loads(line) for line in runs_file.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# FR-5.1 — CLI exit codes
# ---------------------------------------------------------------------------

def test_fr5_1_cli_exit_code_success(config: Config) -> None:
    """main() returns 0 when the pipeline completes without error."""
    _seed_date(config, DATE_A)
    result = main(["--date", DATE_A, "--config", "config/pipeline.yaml"])
    assert result == 0


def test_fr5_1_cli_exit_code_failure(config: Config) -> None:
    """main() returns 1 when a required source column is missing."""
    # Write a CSV that is missing the required `unit_price` column.
    broken_csv = config.landing_orders / f"orders_{DATE_A}.csv"
    with broken_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "product_id", "order_date", "quantity", "status"])
        w.writerow(["ORD-01", "CUST-001", "PROD-001", DATE_A, "1", "shipped"])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    result = main(["--date", DATE_A, "--config", "config/pipeline.yaml"])
    assert result == 1


# ---------------------------------------------------------------------------
# FR-5.2 — Backfill
# ---------------------------------------------------------------------------

def test_fr5_2_backfill_processes_n_plus_one_dates(config: Config) -> None:
    """--backfill 2 with --date DATE_C processes DATE_A, DATE_B, and DATE_C."""
    for d in [DATE_A, DATE_B, DATE_C]:
        _seed_date(config, d)

    main(["--date", DATE_C, "--backfill", "2", "--config", "config/pipeline.yaml"])

    for d in [DATE_A, DATE_B, DATE_C]:
        partition = config.gold / "fact_orders" / f"date={d}" / "data.parquet"
        assert partition.exists(), f"Expected Gold partition missing for {d}"


def test_fr5_2_backfill_order_is_chronological(config: Config) -> None:
    """Backfill runs dates in ascending order: earlier dates come first."""
    for d in [DATE_A, DATE_B]:
        _seed_date(config, d)

    main(["--date", DATE_B, "--backfill", "1", "--config", "config/pipeline.yaml"])

    records = _read_runs(config)
    # Filter to only the two dates touched by this backfill run.
    dates_in_order = [r["date"] for r in records if r["date"] in {DATE_A, DATE_B}]
    assert dates_in_order == [DATE_A, DATE_B], (
        f"Expected chronological order [DATE_A, DATE_B], got {dates_in_order}"
    )


# ---------------------------------------------------------------------------
# FR-5.3 — state/runs.jsonl written on every run
# ---------------------------------------------------------------------------

def test_fr5_3_run_appended_to_runs_jsonl(config: Config) -> None:
    """After one run, state/runs.jsonl exists and contains exactly one JSON line."""
    _seed_date(config, DATE_A)
    run_one_date(DATE_A, config)

    runs_file: Path = config.state / "runs.jsonl"
    assert runs_file.exists(), "state/runs.jsonl was not created"

    lines = [ln for ln in runs_file.read_text().splitlines() if ln.strip()]
    assert len(lines) == 1, f"Expected 1 run record, found {len(lines)}"
    json.loads(lines[0])  # raises if not valid JSON


def test_fr5_3_runs_jsonl_schema(config: Config) -> None:
    """Each run record contains the required metadata keys."""
    _seed_date(config, DATE_A)
    run_one_date(DATE_A, config)

    record = _read_runs(config)[0]
    required_keys = {"date", "status", "started_at", "finished_at", "duration_sec", "stages"}
    missing = required_keys - record.keys()
    assert not missing, f"runs.jsonl record is missing keys: {missing}"


def test_fr5_3_runs_jsonl_accumulates(config: Config) -> None:
    """Running the pipeline twice for the same date appends two records, not one."""
    _seed_date(config, DATE_A)
    run_one_date(DATE_A, config)
    run_one_date(DATE_A, config)

    records = _read_runs(config)
    assert len(records) == 2, (
        f"Expected 2 accumulated run records, found {len(records)}"
    )
