# FR-5: Orchestration — Implementation Plan

## Overview

FR-5 covers CLI orchestration for the NovaCart ETL pipeline. Most of it is already implemented:
- **FR-5.1** (`python -m src.pipeline --date YYYY-MM-DD`) is complete.
- **FR-5.2** (`--backfill N`) is complete and has two integration tests.
- **FR-5.3** (every run appends metadata to `state/runs.jsonl`) has a **filename gap**: the current code writes to `state/run_history.jsonl`, but the spec names the file `state/runs.jsonl`. The metadata schema (date, status, started_at, finished_at, duration_sec, stages) is already correct.

Additionally, there are no dedicated FR-5 tests — the backfill tests in `test_pipeline_scenarios.py` exercise the CLI but never assert on the run-history file. The plan adds a focused test file for FR-5 to close that gap.

**In scope:** Rename the run-history file from `run_history.jsonl` to `runs.jsonl` and add dedicated FR-5 tests.
**Out of scope:** Bronze, Silver, Gold logic; schema changes; any behavioural change to the pipeline stages.

---

## Dependency Flow

1. **Sub-Task 1** — Rename `run_history.jsonl` to `runs.jsonl` in `StateManager`. Must happen first so tests assert on the correct filename.
2. **Sub-Task 2** — Add `tests/test_fr5_orchestration.py` covering FR-5.1, FR-5.2, and FR-5.3. Depends on Sub-Task 1 so the file path asserted in tests is correct.

---

## Sub-Tasks

---

### Sub-Task 1 — FR-5.3: Rename Run-History File to `runs.jsonl`

**Status:** `[ ] pending`

**Intent**
FR-5.3 specifies that every run appends metadata to `state/runs.jsonl`. The current implementation writes to `state/run_history.jsonl`. This single-line rename aligns the output contract with the specification.

**Expected Outcomes**
- `StateManager._runs_file` resolves to `<state_dir>/runs.jsonl`.
- After any pipeline run, `state/runs.jsonl` is created or appended to with a JSON record.
- `state/run_history.jsonl` is no longer created by new runs.
- All existing tests continue to pass (no test currently asserts on the filename `run_history.jsonl`).

**Todo List**
1. In `src/utils/state.py` line 13, change `"run_history.jsonl"` to `"runs.jsonl"`.

**Relevant Context**
- `src/utils/state.py` line 13 — `self._runs_file = self._dir / "run_history.jsonl"` is the only change needed.
- `src/pipeline.py` line 90 calls `state.record_run(metadata)` — no change required there.

---

### Sub-Task 2 — New Tests for FR-5: `tests/test_fr5_orchestration.py`

**Status:** `[ ] pending`

**Intent**
Add a dedicated test file that directly asserts each FR-5 acceptance criterion. The existing backfill tests in `test_pipeline_scenarios.py` prove end-to-end data correctness but never check that `runs.jsonl` is written, its structure, or that the exit code is correct. This test file closes those gaps.

**Expected Outcomes**
- `tests/test_fr5_orchestration.py` exists and all tests pass.
- The following behaviours are verified:
  - **FR-5.1** — `main()` returns exit code `0` on success and `1` on failure.
  - **FR-5.2** — `--backfill N` results in exactly `N + 1` date partitions processed in chronological order.
  - **FR-5.3** — After each run, `state/runs.jsonl` contains a record with the correct `date`, `status`, `started_at`, `finished_at`, `duration_sec`, and `stages` fields. Multiple runs accumulate (append, not overwrite).
- All existing 41 tests continue to pass.

**Todo List**
1. Create `tests/test_fr5_orchestration.py`. Use the `config` fixture from `conftest.py` and helper functions `write_orders_csv`, `write_customers_json`, `make_products_db` to seed input data.
2. Write the following tests:
   - `test_fr5_1_cli_exit_code_success` — run `main()` on valid data; assert return value is `0`.
   - `test_fr5_1_cli_exit_code_failure` — run `main()` with a missing required CSV column; assert return value is `1`.
   - `test_fr5_2_backfill_processes_n_plus_one_dates` — run with `--backfill 2` on three date partitions; assert exactly three Gold fact-orders partitions exist.
   - `test_fr5_2_backfill_order_is_chronological` — run with `--backfill 1`; read `state/runs.jsonl` and assert the two records appear in ascending date order.
   - `test_fr5_3_run_appended_to_runs_jsonl` — run pipeline once; assert `state/runs.jsonl` exists and contains exactly one line that parses as valid JSON.
   - `test_fr5_3_runs_jsonl_schema` — assert the JSON record contains keys: `date`, `status`, `started_at`, `finished_at`, `duration_sec`, `stages`.
   - `test_fr5_3_runs_jsonl_accumulates` — run pipeline twice for the same date; assert `state/runs.jsonl` contains two lines.

**Relevant Context**
- `src/pipeline.py` lines 96-117 — `main()` entry point and `run_one_date()` metadata structure.
- `src/utils/state.py` line 48 — `record_run()` appends to `runs.jsonl` (after Sub-Task 1).
- `tests/conftest.py` — `config` fixture, `write_orders_csv`, `write_customers_json`, `make_products_db`.
- `tests/test_pipeline_scenarios.py` lines 219-249 — existing backfill test pattern to follow. The `config` fixture changes `os.getcwd()` to `tmp_project`, so `--config config/pipeline.yaml` resolves correctly inside `main()`.

---

## Summary of All File Changes

| File | Changed By Sub-Task | Nature of Change |
|---|---|---|
| `src/utils/state.py` | 1 | One-line rename: `run_history.jsonl` to `runs.jsonl` |
| `tests/test_fr5_orchestration.py` | 2 | New file — 7 dedicated FR-5 tests |
| All other files | — | No changes |
