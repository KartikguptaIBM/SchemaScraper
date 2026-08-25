# Silver Validation Layer – FR-3 Implementation Plan

## Overview

Implement FR-3.1 through FR-3.4 for the Silver (validation) layer of the NovaCart ETL pipeline. Most of the behaviour already exists in `src/transform/silver.py`, but there is one correctness gap (FR-3.4: dedup must sort by `_ingested_at` before dropping duplicates, not rely on positional order), and a column naming gap (FR-3.3: the quarantine column is currently called `_quarantine_reason` but the spec calls it `_error_reason`). A new test file and a human-readable description document are also required.

**In scope:** FR-3.1, FR-3.2, FR-3.3, FR-3.4 — production code fix + new tests + test description file.  
**Out of scope:** Bronze ingestion, Gold SCD logic, CLI, config, schema drift detection.

---

## Dependency Flow

1. **Sub-Task 1** — Rename the quarantine column from `_quarantine_reason` to `_error_reason` (FR-3.3 naming fix). Must happen first because the new tests assert on this column name.
2. **Sub-Task 2** — Fix dedup to sort by `_ingested_at` descending before `drop_duplicates`, keeping Bronze metadata available during dedup then stripping it before writing Silver (FR-3.4 correctness fix). Depends on Sub-Task 1 being committed so the file is stable.
3. **Sub-Task 3** — Add new dedicated tests for FR-3.1 through FR-3.4 and write `new_test_descriptions.txt`. Depends on both fixes being in place.

---

## Sub-Tasks

---

### Sub-Task 1 — FR-3.3: Rename Quarantine Column to `_error_reason`

**Status:** `[ ] pending`

**Intent**  
FR-3.3 specifies that quarantined rows must carry an `_error_reason` column. The current implementation writes `_quarantine_reason`. This is a naming mismatch against the requirement. Rename the column so the output contract is correct.

**Expected Outcomes**
- Quarantine parquet files contain an `_error_reason` column (not `_quarantine_reason`).
- `_quarantined_at` column name is unchanged.
- Existing test `test_bad_rows_quarantined` continues to pass (it only checks row count, not column names).

**Todo List**
1. In `src/transform/silver.py`, inside `_validate_df`, change the key assigned in the bad-row dict from `_quarantine_reason` to `_error_reason`.

**Relevant Context**
- `src/transform/silver.py` line 38 — `row_dict["_quarantine_reason"] = str(exc)`.

---

### Sub-Task 2 — FR-3.4: Dedup by `_ingested_at` (latest wins)

**Status:** `[ ] pending`

**Intent**
FR-3.4 requires that when duplicate primary keys exist in Bronze, the row with the latest `_ingested_at` timestamp survives in Silver. Currently `_validate_df` receives a stripped DataFrame (metadata columns removed by `_strip_meta`) so `_ingested_at` is unavailable at dedup time.

The fix introduces a dedicated private function `_dedup_by_ingested_at(df, primary_key, logger, source_name)` that owns all deduplication responsibility. `_validate_df` is also refactored to keep Bronze metadata rows available for that function: it receives the full pre-strip DataFrame, uses the stripped version only for Pydantic, and returns a DataFrame that still has metadata columns. Metadata stripping happens after dedup, inside each `build_silver_*` caller.

**Expected Outcomes**
- A new function `_dedup_by_ingested_at` exists in `silver.py` with a single, clear responsibility.
- When two rows share the same primary key, the one with the later `_ingested_at` value is kept in Silver.
- Metadata columns (`_row_hash`, `_source_file`, `_ingested_at`, `_partition_date`) are absent from the written Silver parquet (stripped by `_strip_meta` in each caller after dedup).
- The quarantine step is unaffected — bad rows are still written with `_error_reason` and `_quarantined_at`.
- The old positional `keep="last"` dedup inside `_validate_df` is removed entirely.

**Todo List**
1. Add a new private function `_dedup_by_ingested_at(df: pd.DataFrame, primary_key: str, logger: logging.Logger, source_name: str) -> pd.DataFrame` in `src/transform/silver.py`:
   - If `_ingested_at` is present in `df.columns` and `primary_key` is present: sort by `_ingested_at` descending, then `drop_duplicates(subset=[primary_key], keep="first")` (first after descending sort = latest timestamp).
   - Log dropped count with `log_event` if any duplicates were removed.
   - Return the resulting DataFrame (metadata columns still intact).
2. Refactor `_validate_df` to receive the full (pre-strip) DataFrame (`df: pd.DataFrame`) instead of an already-stripped one:
   - Internally compute `df_stripped = _strip_meta(df)` at the top of the function.
   - Iterate rows using `df.iterrows()` (full rows) and validate each row against Pydantic using the corresponding stripped row values (`df_stripped.loc[idx]`).
   - If valid, append the **full** row (with metadata) to `good`.
   - Bad rows continue to use the stripped row dict plus `_error_reason` + `_quarantined_at`.
   - Remove the old `drop_duplicates` / dedup block from `_validate_df` — dedup is now the caller's responsibility.
   - Return `pd.DataFrame(good)` with metadata columns still present.
3. In each of the three `build_silver_*` callers:
   - Remove the `_strip_meta()` wrapper from the `_validate_df(...)` call site (pass `df` directly).
   - After `_validate_df` returns, call `_dedup_by_ingested_at(result, primary_key, logger, source_name)`.
   - Then call `_strip_meta(result)` before writing to parquet.

**Relevant Context**
- `src/transform/silver.py` lines 16-59 — `_strip_meta` and `_validate_df`.
- `src/transform/silver.py` lines 74-87, 101-122, 136-150 — three `build_silver_*` callers, each currently call `_validate_df(_strip_meta(df), ...)`.
- `_ingested_at` is a Bronze metadata column added by all three ingest modules.

---

### Sub-Task 3 — New Tests for FR-3 + `new_test_descriptions.txt`

**Status:** `[ ] pending`

**Intent**  
Add a dedicated test file `tests/test_fr3_silver.py` that directly asserts the four FR-3 behaviours at the unit/integration level. Separately, write `new_test_descriptions.txt` with a plain-English description of each test.

**Expected Outcomes**
- `tests/test_fr3_silver.py` exists and all tests in it pass.
- `new_test_descriptions.txt` exists in the project root with a short description per test.
- The new tests cover:
  - **FR-3.1** — every Bronze row is passed to Pydantic (a row with a validation error is caught, a good row passes).
  - **FR-3.2** — valid rows appear in the Silver parquet.
  - **FR-3.3** — invalid rows appear in the quarantine parquet with an `_error_reason` column present.
  - **FR-3.4** — when two rows share a primary key with different `_ingested_at` values, the later timestamp row appears in Silver and the earlier one does not.
- Existing 14 integration tests and 18 schema unit tests continue to pass.

**Todo List**
1. Create `tests/test_fr3_silver.py`. Use `tmp_path` from pytest and call `build_silver_*` directly (unit-style, not full pipeline). Write helper fixtures to build minimal Bronze parquet files with `_ingested_at` metadata.
2. Write the following tests (minimum set — add more if gaps are found during implementation):
   - `test_fr3_1_valid_row_passes_pydantic` — a syntactically valid row survives `_validate_df` and is returned in the result DataFrame.
   - `test_fr3_1_invalid_row_caught_by_pydantic` — a row that violates a Pydantic rule (e.g. `quantity=0`) is absent from the result.
   - `test_fr3_2_valid_rows_written_to_silver` — valid rows appear in the Silver parquet written by `build_silver_orders`.
   - `test_fr3_3_invalid_rows_in_quarantine` — invalid row appears in a quarantine parquet file.
   - `test_fr3_3_error_reason_column_present` — the quarantine parquet contains a column named `_error_reason`.
   - `test_fr3_4_latest_ingested_at_wins` — two rows with same `customer_id` but different `_ingested_at`; only the later-timestamped row is in Silver.
   - `test_fr3_4_earlier_ingested_at_absent` — confirms the earlier-timestamped duplicate is not in Silver.
3. Create `new_test_descriptions.txt` in the project root documenting each test: name, which FR it covers, and one sentence describing what it asserts.

**Relevant Context**
- `src/transform/silver.py` — `_validate_df`, `build_silver_orders`, `build_silver_customers`, `build_silver_products`.
- `tests/conftest.py` — `write_orders_csv`, `write_customers_json`, `make_products_db`, `config` fixture.
- `src/utils/schemas.py` — field types needed to craft valid/invalid Bronze rows for test fixtures.
- Existing scenario tests (`test_pipeline_scenarios.py`) show the pattern for building Bronze + running Silver.

---

## Summary of All File Changes

| File | Changed By Sub-Task | Nature of Change |
|---|---|---|
| `src/transform/silver.py` | 1, 2 | Rename column; restructure `_validate_df` for `_ingested_at`-based dedup |
| `tests/test_fr3_silver.py` | 3 | New file — dedicated FR-3 tests |
| `new_test_descriptions.txt` | 3 | New file — plain-English test descriptions |
| All other files | — | No changes |
