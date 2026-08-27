# FR-4 Gold Layer Plan

## Top-Level Overview

**Goal:** Complete the Gold (star schema) layer of the NovaCart ETL pipeline by:
1. Fixing the `order_date` dtype in `build_fact_orders()` — from `datetime64[ns]` to plain `date`
2. Implementing the missing `dim_date` calendar table (FR-4.1)
3. Wiring `build_dim_date()` into the pipeline orchestrator
4. Writing a dedicated `tests/test_fr4_gold.py` test file covering all four FR-4 sub-requirements

`dim_product` (SCD-1), `dim_customer` (SCD-2), and `fact_orders` (partition-replace) are already implemented in `src/transform/gold.py`. The gaps are: the `order_date` dtype fix, `dim_date`, and the absence of any gold-layer tests.

**Scope:**
- Fix one line in `build_fact_orders()` in `src/transform/gold.py`
- Add `build_dim_date()` to `src/transform/gold.py`
- Add one `stage()` call in `src/pipeline.py` to invoke `build_dim_date()`
- Write `tests/test_fr4_gold.py` with tests for FR-4.1 through FR-4.4

**Non-goals:**
- No changes to Silver layer
- No changes to Bronze ingestion
- No foreign-key enforcement or join logic in the pipeline itself

---

## Known Data Type Context

Before implementation, note these type facts discovered from code inspection:

- `fact_orders.order_date` is currently written as **`datetime64[ns]`** (pandas Timestamp) because `gold.py` line 141 calls `pd.to_datetime()` on the silver `date` object — this over-promotes a calendar date to a full timestamp unnecessarily
- **Decided fix:** change `pd.to_datetime(df["order_date"])` to `pd.to_datetime(df["order_date"]).dt.date` so `order_date` is stored as a plain **`datetime.date`** (Parquet INT32 DATE type) — semantically correct, compact, and consistent with what silver already stores
- `fact_orders.total_amount` is **`float64`** — product of `int64 quantity × float64 unit_price` — correct, no change needed
- `dim_date.date_key` must be stored as **`datetime.date`** (same Parquet INT32 DATE type) to allow clean zero-cast equi-joins with the fixed `fact_orders.order_date`
- Pandas stores Python `date` objects using `object` dtype in DataFrames — tests must use `isinstance(df["order_date"].iloc[0], date)` rather than checking `dtype == "datetime64[ns]"`

---

## Sub-Tasks

---

### Sub-Task 1 — Fix `order_date` dtype and implement `build_dim_date()` in `src/transform/gold.py`

**Status:** `[ ] pending`

**Intent:**
Two changes to `src/transform/gold.py`:

1. **Fix `build_fact_orders()`** — change `pd.to_datetime(df["order_date"])` to `pd.to_datetime(df["order_date"]).dt.date` so `order_date` is stored as a plain Python `date` (Parquet INT32 DATE) rather than a full Timestamp. This is semantically correct for a calendar date and aligns the fact table join key with `dim_date.date_key`.

2. **Add `build_dim_date()`** — generate a static calendar dimension covering every day from 2024-01-01 through 2026-12-31, write it to `gold/dim_date.parquet`. This table is generated from code, not from any source system. It is idempotent — re-running always produces the same output.

**Expected Outcomes:**
- `data/gold/dim_date.parquet` exists after the pipeline runs
- Contains one row per calendar day for 2024–2026 (1096 rows: 366 + 365 + 365)
- Columns present:
  - `date_key` — `datetime.date` (Parquet INT32 DATE), the primary join key matching `fact_orders.order_date`
  - `year` — int (e.g. 2025)
  - `month` — int (1–12)
  - `day` — int (1–31)
  - `quarter` — int (1–4)
  - `week` — int (ISO week number, 1–53)
  - `day_of_week` — int (0=Monday … 6=Sunday, pandas default)
  - `day_name` — str (e.g. "Monday")
  - `month_name` — str (e.g. "November")
  - `is_weekend` — bool (True if day_of_week >= 5)
- Function signature: `build_dim_date(gold_dir: Path, logger: logging.Logger) -> Path`
- Log event emitted: `dim_date_written` with `rows=` count
- If the file already exists it is **overwritten** (static data, always idempotent)
- `fact_orders.order_date` is stored as Parquet INT32 DATE (not INT64 Timestamp) after the fix

**Todo List:**
1. In `build_fact_orders()` at line 141 of `src/transform/gold.py`, change:
   ```python
   df["order_date"] = pd.to_datetime(df["order_date"])
   ```
   to:
   ```python
   df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
   ```
2. Add `build_dim_date()` after the `_row_hash` helper and before `build_dim_product`
3. Inside `build_dim_date()`, use `pd.date_range(start="2024-01-01", end="2026-12-31", freq="D")` to generate a `DatetimeIndex`
4. Build a DataFrame with all required columns derived using pandas `.dt` accessors on that `DatetimeIndex`
5. Set `date_key` column by calling `.dt.date` on the series **last** — this converts each element to a Python `datetime.date` object (Parquet INT32 DATE); note that `.dt` accessor is not available after this conversion so all other columns must be derived before this step
6. Write to `gold_dir / "dim_date.parquet"` with `index=False`
7. Emit a `log_event` with event name `"dim_date_written"` and `rows=len(df)`

**Relevant Context:**
- File to edit: `src/transform/gold.py`
- `build_fact_orders()` starts at line 122; the line to fix is line 141
- Pattern to follow: `build_dim_product()` at line 22 — same `gold_dir.mkdir`, `to_parquet`, `log_event` pattern
- No source parquet is read for `dim_date` — data is generated entirely from `pd.date_range`
- No new imports needed — `pandas`, `logging`, and `Path` are already imported in `gold.py`

---

### Sub-Task 2 — Wire `build_dim_date()` into `src/pipeline.py`

**Status:** `[ ] pending`

**Intent:**
Make `dim_date` part of every pipeline run. Since it is a static calendar, it should be built once per run (not per date). It is safe to call on every run because it is fully idempotent.

**Expected Outcomes:**
- `src/pipeline.py` imports `build_dim_date` from `src.transform.gold`
- A `stage("dim_date", ...)` call exists in `run_one_date()` in the Gold section
- Running `python -m src.pipeline --date 2025-11-08` produces `data/gold/dim_date.parquet`

**Todo List:**
1. Add `build_dim_date` to the existing import from `src.transform.gold` at lines 24–28 of `src/pipeline.py`
2. In the Gold section of `run_one_date()` (at line 67, before the existing `dim_product` stage), add:
   ```python
   stage("dim_date", lambda: build_dim_date(config.gold, logger))
   ```
3. Final stage order: `dim_date` → `dim_product` → `dim_customer` → `fact_orders`

**Relevant Context:**
- File to edit: `src/pipeline.py`
- Existing gold stage block is at lines 67–74
- `config.gold` is the gold `Path` object — same argument already used by `build_dim_product`

---

### Sub-Task 3 — Write `tests/test_fr4_gold.py`

**Status:** `[ ] pending`

**Intent:**
Provide dedicated, focused unit tests for each FR-4 sub-requirement, mirroring the structure and style of `tests/test_fr3_silver.py`. Each test calls the gold builder functions directly against `tmp_path` fixtures — no full pipeline invocation.

**Expected Outcomes:**
- File `tests/test_fr4_gold.py` exists and all tests pass with `pytest`
- Tests cover:
  - FR-4.1: `dim_date` row count (1096), column presence, `date_key` type is `datetime.date`, boundary dates present
  - FR-4.2: `dim_product` SCD-1 — a re-run with an updated product overwrites the old value; only one row per `product_id`
  - FR-4.3: `dim_customer` SCD-2 — tracked field change closes old row and inserts new current row; unchanged customer keeps one row; new customer on second load is inserted
  - FR-4.4: `fact_orders` idempotency — running twice for same date produces same row count; `order_date` is a plain `date` object; `total_amount` dtype is `float64`; `total_amount` values are correct

**Todo List:**

1. Create `tests/test_fr4_gold.py` with module docstring referencing FR-4.1–FR-4.4

2. Add shared helpers (following `test_fr3_silver.py` patterns):
   - `_logger()` — returns a standard logger
   - `_write_silver_orders(silver_dir, date_str, rows)` — writes a minimal silver orders parquet with no `_`-prefixed columns; `quantity` as int, `unit_price` as float, `order_date` as date string
   - `_write_silver_customers(silver_dir, rows)` — writes silver customers parquet
   - `_write_silver_products(silver_dir, rows)` — writes silver products parquet
   - `_good_product(product_id, name, unit_cost)` — returns a minimal product row dict
   - `_good_customer(customer_id, city, country, email)` — returns a minimal customer row dict
   - `_good_order(order_id, quantity, unit_price, date_str)` — returns a minimal order row dict

3. **FR-4.1 tests** (`dim_date`):
   - `test_fr4_1_dim_date_row_count` — assert `len(df) == 1096`
   - `test_fr4_1_dim_date_columns_present` — assert all required columns are present
   - `test_fr4_1_dim_date_key_is_date_type` — assert `isinstance(df["date_key"].iloc[0], date)` (Python `date` object)
   - `test_fr4_1_dim_date_boundary_dates` — assert `date(2024, 1, 1)` and `date(2026, 12, 31)` are present in `date_key`

4. **FR-4.2 tests** (`dim_product` SCD-1):
   - `test_fr4_2_scd1_initial_load` — first load writes all products to `dim_product.parquet`
   - `test_fr4_2_scd1_overwrite_on_collision` — second run with updated `unit_cost` overwrites; only one row per `product_id` in result; new value present, old value absent

5. **FR-4.3 tests** (`dim_customer` SCD-2):
   - `test_fr4_3_scd2_initial_load` — first load: all customers have `_current=True`, `_eff_end="9999-12-31"`
   - `test_fr4_3_scd2_unchanged_customer_no_new_row` — re-run with same data: still one row per customer
   - `test_fr4_3_scd2_tracked_field_change_closes_old_row` — change `city` on existing customer: old row has `_current=False` and `_eff_end` set to today; new row has `_current=True`
   - `test_fr4_3_scd2_new_customer_inserted` — customer not present in first load appears in second load as a new `_current=True` row

6. **FR-4.4 tests** (`fact_orders` idempotency):
   - `test_fr4_4_fact_orders_written_at_correct_path` — output parquet exists at `gold/fact_orders/date={date}/data.parquet`
   - `test_fr4_4_fact_orders_idempotent` — running `build_fact_orders` twice for same date produces same row count
   - `test_fr4_4_order_date_is_plain_date` — assert `isinstance(df["order_date"].iloc[0], date)` (not a Timestamp)
   - `test_fr4_4_total_amount_dtype_is_float64` — assert `df["total_amount"].dtype == "float64"`
   - `test_fr4_4_total_amount_value_correct` — assert `total_amount == quantity * unit_price` for a known row

**Relevant Context:**
- Pattern file: `tests/test_fr3_silver.py` — copy the helper/fixture style exactly
- Imports needed: `build_dim_date`, `build_dim_product`, `build_dim_customer`, `build_fact_orders` from `src.transform.gold`
- Import `from datetime import date` — needed for `isinstance` checks in FR-4.1 and FR-4.4 dtype tests
- `scd2_fields` argument for `build_dim_customer` — use `["city", "country", "email"]` matching `config/pipeline.yaml`
- Silver parquet fixtures must NOT include `_`-prefixed columns (silver has already stripped metadata)
- `order_date` in silver fixtures should be a plain date string (e.g. `"2025-11-08"`) — `build_fact_orders` applies `pd.to_datetime().dt.date` internally
- `_HIGH_DATE = "9999-12-31"` is a module constant in `gold.py` — reference this value literally in SCD-2 assertions

---

## Plan Validation Checklist

- [x] `dim_date` columns confirmed: `date_key`, `year`, `month`, `day`, `quarter`, `week`, `day_of_week`, `day_name`, `month_name`, `is_weekend`
- [x] `date_key` dtype decided: plain `datetime.date` (Parquet INT32 DATE) — matches fixed `fact_orders.order_date`
- [x] `order_date` fix decided: `pd.to_datetime().dt.date` in `build_fact_orders()`
- [x] `dim_date` regeneration decided: **always overwrite** — matches every other gold builder; negligible cost for 1096 rows; eliminates stale-data risk if columns are ever added
- [x] 13 test cases confirmed sufficient for FR-4.1–FR-4.4 coverage
- [x] Date range decided: **hardcoded** as `"2024-01-01"` / `"2026-12-31"` — matches FR-4.1 spec exactly; extension point is `config.gold_cfg.get()` in `pipeline.py` if 2027 support is ever needed (no `Config` changes required, just two YAML keys)

## ✅ Plan Complete — Ready for Implementation
