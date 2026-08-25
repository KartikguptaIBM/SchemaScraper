# Bronze Layer Ingestion – FR-1 Implementation Plan

## Overview

Implement six functional requirements for the Bronze (ingestion) layer of the NovaCart ETL pipeline. The changes span three ingest modules (`orders.py`, `customers.py`, `products.py`), the Silver transformation module (`silver.py`), and the customer schema contract (`schemas.py`). Downstream Gold logic is unaffected.

**In scope:** FR-1.1 through FR-1.6 only.  
**Out of scope:** Silver validation rules, Gold SCD logic, CLI, config, tests beyond what is needed to keep existing tests green.

---

## Dependency Flow

FR-1.2 (stop flattening at Bronze) requires a compensating change in Silver so that customers still reach Gold with flat `city`/`country` columns. The sub-tasks are ordered to respect this dependency:

1. FR-1.1 — encoding fallback (self-contained, orders only)
2. FR-1.4 + FR-1.5 — `_row_hash` and string-dtype (all three ingest files; must land before Silver pre-cast)
3. FR-1.2 — Bronze no longer flattens; Silver gains the flatten step (requires FR-1.5 to be done first so Bronze columns are known)
4. FR-1.3 — watermark confirmation + `_source_file` addition to products (verifies existing logic, adds one column)
5. Silver pre-cast — add type-coercion step in Silver to handle all-string Bronze (required by FR-1.5; blocked until FR-1.5 Bronze changes are done)
6. FR-1.6 — partitioning verification (orders already done; confirm customers/products remain unpartitioned per user clarification)

---

## Sub-Tasks

---

### Sub-Task 1 — FR-1.1: UTF-8 + Latin-1 Fallback for Orders CSV

**Status:** `[ ] pending`

**Intent**  
The orders CSV files may arrive with non-UTF-8 characters (e.g. accented names, special symbols encoded in Latin-1). A hard UTF-8 read will raise a `UnicodeDecodeError`. Add a try/except that retries with `latin-1` before failing.

**Expected Outcomes**
- `pd.read_csv(src)` is replaced by a helper that tries `utf-8` first, then falls back to `latin-1`.
- If both encodings fail, an `IngestionError` is raised.
- Existing tests continue to pass (they use plain ASCII CSVs, so UTF-8 path is always taken).

**Todo List**
1. In `src/ingest/orders.py`, replace the single `pd.read_csv(src)` call with a try/except block:
   - First attempt: `pd.read_csv(src, encoding="utf-8")`
   - On `UnicodeDecodeError`: retry with `pd.read_csv(src, encoding="latin-1")` and emit an `INFO` log event `orders_encoding_fallback`.
   - On a second failure: re-raise as `IngestionError`.

**Relevant Context**
- `src/ingest/orders.py` line 30 — current `pd.read_csv(src)` call.
- `src/utils/logging_setup.py` — `log_event(logger, level, event, **kwargs)` pattern.
- `src/utils/exceptions.py` — `IngestionError`.

---

### Sub-Task 2 — FR-1.4 + FR-1.5: `_row_hash` Column + All-String Bronze Dtypes

**Status:** `[ ] pending`

**Intent**  
Every Bronze row must carry a `_row_hash` fingerprint (SHA-256 of the business-column values) so downstream deduplication and audit can detect mutations. Separately, Bronze must write every column as Python `str` (no Pandas type inference) so the layer is a faithful raw snapshot; Silver handles all type coercion.

Both concerns are addressed together because the hash must be computed from the raw string values (before any type coercion), and computing the hash after casting to strings is the natural order.

**Expected Outcomes**
- All three ingest modules add a `_row_hash` column to every output row.
- The hash is a hex SHA-256 digest of the concatenated string values of the business columns (columns not starting with `_`) in a deterministic column order.
- All columns (business + metadata) are written as `dtype=str` / `object` in the output parquet.
- Products ingest also adds `_source_file = Path(db_path).name` (currently missing, required by FR-1.4).
- No existing Silver Pydantic validation logic changes in this sub-task (that is Sub-Task 5).

**Todo List**
1. Create a small private helper function `_compute_row_hash(row: pd.Series, cols: list[str]) -> str` in each ingest file (or shared utility). It should:
   - Select only the `cols` columns from `row`.
   - Cast each value to `str`, join with `|`, and return `hashlib.sha256(joined.encode()).hexdigest()`.
2. In `src/ingest/orders.py`:
   - After reading and schema-checking, cast all columns to `str` using `df = df.astype(str)`.
   - Compute `df["_row_hash"]` using the helper over the business columns (`EXPECTED_COLUMNS`).
   - Then attach `_source_file`, `_ingested_at`, `_partition_date`.
3. In `src/ingest/customers.py`:
   - After reading JSON into a DataFrame and schema-checking, cast to `str`.
   - Add `_row_hash` over `EXPECTED_COLUMNS`.
   - Then attach `_source_file`, `_ingested_at`.
4. In `src/ingest/products.py`:
   - After reading from SQLite, cast to `str`.
   - Add `_source_file = Path(db_path).name`.
   - Add `_row_hash` over `EXPECTED_COLUMNS`.
   - Then attach `_ingested_at`.
   - Update watermark advancement to use the string column value (already a string after cast).

**Relevant Context**
- `src/ingest/orders.py` lines 35–43.
- `src/ingest/customers.py` lines 45–51.
- `src/ingest/products.py` lines 57–65.
- `hashlib` is in the Python standard library — no new dependency.

---

### Sub-Task 3 — FR-1.2: Customers JSON — Preserve Nested Structure at Bronze

**Status:** `[ ] pending`

**Intent**  
The Bronze layer must be a faithful raw snapshot. Currently `ingest_customers` pops the `address` dict and promotes `city`/`country` to top-level columns, discarding the original nesting. Per FR-1.2 and user decision, Bronze must store `address` as a JSON string column and NOT produce flat `city`/`country` columns. Flattening moves to Silver.

**Expected Outcomes**
- Bronze customers parquet contains an `address` column (JSON string, e.g. `'{"city": "NYC", "country": "US"}'`) instead of `city` / `country`.
- `EXPECTED_COLUMNS` in `customers.py` is updated to replace `city`/`country` with `address`.
- `build_silver_customers` in `src/transform/silver.py` reads `address` from Bronze, parses it, and expands `city`/`country` before Pydantic validation.
- `CustomerRow` in `src/utils/schemas.py` continues to expect flat `city`/`country` (unchanged) since Silver flattens before calling Pydantic.
- All 14 existing acceptance tests continue to pass.

**Todo List**
1. In `src/ingest/customers.py`:
   - Remove the flattening loop (`rec.pop("address", {})` / `rec["city"]` / `rec["country"]`).
   - Instead, for each record serialize the `address` value to a JSON string: `rec["address"] = json.dumps(rec.get("address", {}))`.
   - Update `EXPECTED_COLUMNS` to `["customer_id", "first_name", "last_name", "email", "address", "signup_date", "tier"]`.
2. In `src/transform/silver.py`, in `build_silver_customers` before calling `_validate_df`:
   - Parse the `address` JSON string column into `city`/`country` columns.
   - Drop the `address` column after expansion.
   - Handle malformed JSON by defaulting `city`/`country` to `""`.

**Relevant Context**
- `src/ingest/customers.py` lines 32–38 — current flatten loop.
- `src/transform/silver.py` lines 78–97 — `build_silver_customers`.
- `src/utils/schemas.py` lines 42–57 — `CustomerRow` (city, country remain here, unchanged).
- `tests/conftest.py` line 62 — `write_customers_json` writes `"address": {"city": ..., "country": ...}` already.

---

### Sub-Task 4 — FR-1.3 Verification: Products SQLite Incremental with Watermark

**Status:** `[ ] pending`

**Intent**  
FR-1.3 requires incremental products ingestion with a watermark. This already exists. This sub-task confirms the implementation is correct and complete after Sub-Task 2 changes, and marks FR-1.3 done.

**Expected Outcomes**
- `src/ingest/products.py` uses `WHERE updated_at > ?` with a persisted watermark — confirmed correct.
- Watermark is advanced only after a successful parquet write — confirmed correct.
- Empty result case (no new rows) returns without writing or advancing watermark — confirmed correct.
- FR-1.3 is marked done; no code changes required beyond what Sub-Task 2 already adds.

**Todo List**
1. Re-read `src/ingest/products.py` after Sub-Task 2 changes are applied.
2. Confirm watermark read → SQL filter → parquet write → watermark advance flow is intact.
3. Confirm `_source_file` (added in Sub-Task 2) uses `Path(db_path).name`.
4. Mark FR-1.3 complete with no further changes.

**Relevant Context**
- `src/ingest/products.py` lines 34–68.
- `src/utils/state.py` — `StateManager.get_watermark` / `set_watermark`.

---

### Sub-Task 5 — Silver Pre-Cast: Type Coercion After All-String Bronze

**Status:** `[ ] pending`

**Intent**  
FR-1.5 makes Bronze write all columns as strings. The Silver Pydantic models still expect typed fields (`quantity: int`, `unit_price: float`, `unit_cost: float`). Without a cast step, Pydantic validation will fail for every row. A pre-cast step in each `build_silver_*` function converts string columns back to their target types before rows are passed to `_validate_df`. Metadata columns (`_source_file`, `_ingested_at`, `_row_hash`, `_partition_date`) must also be stripped before Pydantic sees the rows.

**Expected Outcomes**
- `build_silver_orders` casts `quantity` → int, `unit_price` → float before Pydantic validation.
- `build_silver_customers` needs no numeric cast (all fields are strings or parsed by Pydantic from strings).
- `build_silver_products` casts `unit_cost` → float before Pydantic validation.
- Metadata columns are dropped from the DataFrame before `_validate_df` is called.
- All 14 existing acceptance tests pass.

**Todo List**
1. In `src/transform/silver.py`, add a helper `_strip_meta(df)` that returns a copy of `df` with all columns starting with `_` dropped.
2. In `build_silver_orders` before calling `_validate_df`:
   - Cast `df["quantity"]` = `pd.to_numeric(df["quantity"], errors="coerce")`.
   - Cast `df["unit_price"]` = `pd.to_numeric(df["unit_price"], errors="coerce")`.
   - Call `_validate_df(_strip_meta(df), ...)`.
3. In `build_silver_customers` before calling `_validate_df`:
   - Call `_validate_df(_strip_meta(df), ...)` (after address expansion from Sub-Task 3).
4. In `build_silver_products` before calling `_validate_df`:
   - Cast `df["unit_cost"]` = `pd.to_numeric(df["unit_cost"], errors="coerce")`.
   - Call `_validate_df(_strip_meta(df), ...)`.

**Relevant Context**
- `src/transform/silver.py` — all three `build_silver_*` functions.
- `src/utils/schemas.py` — `OrderRow`, `CustomerRow`, `ProductRow` field types.
- Pydantic v2 parses `date` from ISO string automatically — no special handling needed for date string fields.

---

### Sub-Task 6 — FR-1.6 Verification: Bronze Partitioned by Date

**Status:** `[ ] pending`

**Intent**  
FR-1.6 requires Bronze to be partitioned by date. Per user clarification, this applies only to Orders (the only source with a natural date key). Customers and Products remain single unpartitioned files. This sub-task verifies that orders partitioning is correct and documents the scope decision.

**Expected Outcomes**
- Orders Bronze is written to `data/bronze/orders/date=YYYY-MM-DD/data.parquet` — confirmed correct (already implemented).
- Customers and Products Bronze remain at `data/bronze/customers/data.parquet` and `data/bronze/products/data.parquet` — confirmed correct, no change required.
- FR-1.6 is marked complete with no code changes required.

**Todo List**
1. Re-read `src/ingest/orders.py` after Sub-Task 2 changes, confirm Hive-style partition path `date={date_str}` is intact.
2. Confirm Customers and Products remain unpartitioned.
3. Mark FR-1.6 complete.

**Relevant Context**
- `src/ingest/orders.py` lines 40–43.
- User decision: only partitionable sources (Orders) receive date partitioning.

---

## Summary of All File Changes

| File | Changed By Sub-Tasks |
|---|---|
| `src/ingest/orders.py` | 1, 2 |
| `src/ingest/customers.py` | 2, 3 |
| `src/ingest/products.py` | 2 |
| `src/transform/silver.py` | 3, 5 |
| `src/utils/schemas.py` | No changes |
| `tests/conftest.py` | No changes |
| `tests/test_pipeline_scenarios.py` | No changes |
