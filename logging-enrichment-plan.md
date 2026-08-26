# Logging Enrichment Plan — FR-6.1 / FR-6.2

## Overview

Enrich the existing structured JSON logging to satisfy:

- **FR-6.1** — All events logged as JSON to `logs/pipeline.jsonl` (already partially met; gaps closed here)
- **FR-6.2** — Every log event includes `run_id`, `stage`, and `rows_in` / `rows_out` / `rows_quarantined` where the values are known

**Approach:** Minimal, additive changes only. `run_id` is generated once in `pipeline.py` and threaded through every `log_event` call-site via the `log_event` signature. `rows_in`, `rows_out`, `rows_quarantined` are added to events that already carry row-count information. No new abstractions, no structural refactor.

**Confirmed design decisions:**
- Bronze ingest events hardcode `rows_quarantined=0` (bronze has no validation step)
- `run_id` is stored in **both** `logs/pipeline.jsonl` (via `log_event`) **and** `state/run_history.jsonl` (added to the metadata dict that `StateManager.record_run()` persists)

---

## Sub-Tasks

---

### Sub-Task 1 — Add `run_id` to `log_event` and propagate through all call-sites

**Intent**
Every log line needs a `run_id` so events from a single pipeline execution can be correlated. `run_id` is generated once per `run_one_date()` call in `pipeline.py` and passed as a kwarg into every `log_event` invocation.

**Expected Outcomes**
- `log_event` accepts `run_id` naturally via `**kwargs` (no signature change needed — it already uses `**kwargs`)
- `run_id` (UUID4 string) is generated in `pipeline.py:run_one_date()` before the first stage runs
- All 25+ `log_event` call-sites in ingest/transform modules receive `run_id=run_id` as an extra kwarg
- Every line in `logs/pipeline.jsonl` contains a `"run_id"` field
- `schema_check.py`'s bare `logger.warning()` call is converted to `log_event` so it also carries `run_id`

**Todo List**
1. In `src/utils/logging_setup.py` — no change needed; `**kwargs` already forwards arbitrary fields
2. In `src/pipeline.py:run_one_date()` — generate `run_id = str(uuid.uuid4())` at the top of the function; import `uuid`; pass `run_id=run_id` to the existing `log_event(…"pipeline_end"…)` call; thread `run_id` into every `stage()` lambda by updating each ingest/transform function signature to accept `run_id`
3. In `src/ingest/orders.py` — add `run_id: str` param to `ingest_orders()`; pass `run_id=run_id` to all 3 `log_event` calls in that file
4. In `src/ingest/customers.py` — same pattern as orders
5. In `src/ingest/products.py` — same pattern as orders (4 call-sites)
6. In `src/transform/silver.py` — add `run_id: str` to `_validate_df()`, `_dedup_by_ingested_at()`, and the three public `build_silver_*` functions; pass `run_id=run_id` to all 8 `log_event` calls
7. In `src/transform/gold.py` — add `run_id: str` to all three public `build_*` functions; pass to all 7 `log_event` calls
8. In `src/transform/schema_check.py` — replace `logger.warning(f"…")` with `log_event(logger, "WARNING", "schema_drift_additive", source=source_name, added_columns=sorted(added), run_id=run_id)`; update `check_schema()` signature to accept `run_id: str`; update all callers (`orders.py`, `customers.py`, `products.py`)
9. In `src/pipeline.py` — add `run_id=run_id` to the metadata dict passed to `state.record_run()` so it is persisted to `state/run_history.jsonl`

**Relevant Context**
- `src/utils/logging_setup.py` — `log_event()` already uses `**kwargs`, so `run_id` flows in automatically
- `src/pipeline.py:run_one_date()` — the single orchestration entry-point; `run_id` should be created here
- All `log_event` call-sites inventoried in the sub-agent report above

**Status:** [x] done

---

### Sub-Task 2 — Add `stage`, `rows_in`, `rows_out`, `rows_quarantined` to measurable events

**Intent**
Every event that already tracks row counts is enriched with the full `rows_in`, `rows_out`, `rows_quarantined` triple (using `None` / omitting the field when not applicable) and a `stage` label identifying which pipeline stage the event belongs to.

**Expected Outcomes**
- Terminal ingest events (`orders_bronze_written`, `customers_bronze_written`, `products_*`) include `stage="bronze"`, `rows_in=<raw rows>`, `rows_out=<written rows>`, `rows_quarantined=0` (bronze has no quarantine)
- Silver terminal events (`silver_orders_written`, etc.) include `stage="silver"`, `rows_in=<rows entering validation>`, `rows_out=<rows written>`, `rows_quarantined=<count rejected>`
- Silver intermediate events (`_quarantined`, `_duplicates_quarantined`, `_deduped`) include `stage="silver"` and applicable subset of fields
- Gold terminal events (`dim_product_written`, `dim_customer_written`, `fact_orders_written`) include `stage="gold"` and `rows_in=rows_out=len(df)` (gold has no quarantine)
- `pipeline_end` event in `pipeline.py` keeps its existing fields; a `stage="pipeline"` label is added

**Todo List**
1. In `src/ingest/orders.py:ingest_orders()` — capture `rows_in = len(df)` after reading CSV; update `orders_bronze_written` event to include `stage="bronze"`, `rows_in=rows_in`, `rows_out=len(df)`, `rows_quarantined=0` (hardcoded)
2. In `src/ingest/customers.py:ingest_customers()` — same pattern; `rows_in` captured after `pd.DataFrame(rows)`, `rows_quarantined=0` (hardcoded)
3. In `src/ingest/products.py:ingest_products()` — `rows_in` captured after `pd.read_sql_query`; update `products_ingested` event to include `stage="bronze"`, `rows_in`, `rows_out=len(df)`, `rows_quarantined=0` (hardcoded)
4. In `src/transform/silver.py:_validate_df()` — function already knows `len(df)` (rows in) and `len(good)` (rows out) and `len(bad)` (quarantined); pass these through to the `_quarantined` log event; return a small named-tuple or update the existing `_quarantined` event to include `rows_in`, `rows_out`, `rows_quarantined`
5. In `src/transform/silver.py:_dedup_by_ingested_at()` — update `_deduped` event to include `stage="silver"`, `rows_in=len(df)`, `rows_out=len(deduped)`, `rows_quarantined=dupes`
6. In `src/transform/silver.py:build_silver_orders/customers/products()` — capture `rows_in = len(df)` before `_validate_df`; after dedup and strip, update the terminal `silver_*_written` event to include `stage="silver"`, `rows_in`, `rows_out=len(df)`, `rows_quarantined` (sum of validate + dedup quarantine counts); requires `_validate_df` and `_dedup_by_ingested_at` to return their quarantine counts
7. In `src/transform/gold.py:build_dim_product/dim_customer/build_fact_orders()` — add `stage="gold"` and `rows_in=rows_out=len(df)`, `rows_quarantined=0` (hardcoded) to all terminal `_written` events
8. In `src/pipeline.py` — add `stage="pipeline"` to the `pipeline_end` log event

**Relevant Context**
- `src/transform/silver.py:_validate_df()` returns a DataFrame; it currently only logs quarantine count internally — the count needs to be surfaced to the caller (return value change or separate counter)
- `src/transform/silver.py:_dedup_by_ingested_at()` same: currently logs internally; count needs to be surfaced
- Minimal approach: change both private functions to return `(df, quarantined_count: int)` instead of just `df`; update the three `build_silver_*` callers accordingly
- `rows_quarantined=0` hardcoded for bronze and gold events (confirmed decision)
- `run_id` written to both `logs/pipeline.jsonl` and `state/run_history.jsonl` (confirmed decision)

**Status:** [x] done

---

## File Change Summary

| File | Changes |
|------|---------|
| `src/utils/logging_setup.py` | No change |
| `src/pipeline.py` | Import `uuid`; generate `run_id`; thread into all stage lambdas; add `stage`, `run_id` to `pipeline_end` event |
| `src/ingest/orders.py` | Add `run_id` param; add `stage`, `rows_in/out/quarantined` to events |
| `src/ingest/customers.py` | Same as orders |
| `src/ingest/products.py` | Same as orders |
| `src/transform/schema_check.py` | Replace bare `logger.warning` with `log_event`; add `run_id` param |
| `src/transform/silver.py` | Add `run_id` param; `_validate_df` and `_dedup_by_ingested_at` return `(df, int)`; enrich all events |
| `src/transform/gold.py` | Add `run_id` param; enrich terminal events with `stage`, `rows_in/out/quarantined` |
