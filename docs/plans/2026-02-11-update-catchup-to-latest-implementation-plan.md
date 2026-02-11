# Update Catch-Up To Latest Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make default `update` catch up from local timestamp to API latest date in one run with strict stop on real failures.

**Architecture:** Introduce date-queue resolution before per-product sync. Use `latest` candidates first and fallback probe when needed. Reuse existing single-date `process_product` in a loop and persist status after each successful date.

**Tech Stack:** Python 3, Typer CLI, requests, unittest

---

### Task 1: Add failing tests for catch-up date parsing and queue resolution

**Files:**
- Modify: `/Users/yuhan/workspace/quant/data/quantclass-sync/tests/test_default_entry_update.py`

**Step 1: Write the failing test**

Add tests for:
- parsing multi-date latest payload into normalized sorted unique list
- building queue from local date to latest candidates

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_default_entry_update.DefaultEntryUpdateTests.test_latest_parser_returns_sorted_unique_dates`

Expected: FAIL due to missing helper behavior.

**Step 3: Write minimal implementation**

Add helper functions in `quantclass_sync.py` for:
- parsing latest candidates list
- computing catch-up queue from local date

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_default_entry_update.DefaultEntryUpdateTests.test_latest_parser_returns_sorted_unique_dates`

Expected: PASS.

### Task 2: Add failing tests for strict stop in multi-date execution

**Files:**
- Create: `/Users/yuhan/workspace/quant/data/quantclass-sync/tests/test_update_catchup.py`

**Step 1: Write the failing test**

Add tests for:
- multi-date queue executes in ascending order
- hard failure on day N stops day N+1
- timestamp write reflects last successful day only

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_update_catchup -v`

Expected: FAIL showing behavior not implemented yet.

**Step 3: Write minimal implementation**

Update `_execute_plans` and helpers to iterate `date_queue` per product.

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_update_catchup -v`

Expected: PASS.

### Task 3: Add fallback probe behavior and no-data-day skip test

**Files:**
- Modify: `/Users/yuhan/workspace/quant/data/quantclass-sync/tests/test_update_catchup.py`
- Modify: `/Users/yuhan/workspace/quant/data/quantclass-sync/quantclass_sync.py`

**Step 1: Write the failing test**

Add test verifying probe fallback skips no-data days and still catches up.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_update_catchup.UpdateCatchUpTests.test_probe_fallback_skips_no_data_days`

Expected: FAIL due to missing probe fallback behavior.

**Step 3: Write minimal implementation**

Add probe helper and integrate with queue resolver.

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_update_catchup.UpdateCatchUpTests.test_probe_fallback_skips_no_data_days`

Expected: PASS.

### Task 4: Update documentation and run full verification

**Files:**
- Modify: `/Users/yuhan/workspace/quant/data/quantclass-sync/README.md`

**Step 1: Update docs**

Document that default `update` now catches up from local timestamp to API latest.

**Step 2: Run verification**

Run:
- `python3 -m unittest tests.test_default_entry_update -v`
- `python3 -m unittest tests.test_update_catchup -v`
- `python3 -m unittest discover -s tests -p 'test_*.py'`

Expected: all pass.

