# Metabase Encoding And Width Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix garbled Chinese text in the BI dashboard and make the dashboard use full-width layout.

**Architecture:** Correct the CSV fallback seeding path so MySQL inserts use `utf8mb4`, then reseed the ADS tables to replace corrupted rows. Update the Metabase dashboard initialization flow so the dashboard width is set to full-width whenever it is created or synced.

**Tech Stack:** Python 3, unittest, MySQL CLI, Metabase REST API

### Task 1: Add failing regression tests

**Files:**
- Modify: `warehouse/tests/test_seed_ads_from_csv.py`
- Modify: `warehouse/tests/test_init_metabase_dashboard.py`

**Step 1: Write a failing test for MySQL charset handling**
- Assert the MySQL CLI command includes `--default-character-set=utf8mb4`.

**Step 2: Write a failing test for dashboard width sync**
- Assert dashboard update requests include `width: full`.

**Step 3: Run tests to verify they fail**
- Run: `python3 -m unittest warehouse.tests.test_seed_ads_from_csv warehouse.tests.test_init_metabase_dashboard`

### Task 2: Implement minimal fixes

**Files:**
- Modify: `warehouse/scripts/seed_ads_from_csv.py`
- Modify: `warehouse/scripts/init_metabase_dashboard.py`

**Step 1: Update MySQL CLI invocation**
- Add `--default-character-set=utf8mb4` to the script's mysql command.

**Step 2: Update dashboard sync logic**
- Ensure the dashboard is updated to `width: full`.

### Task 3: Verify live behavior

**Files:**
- Modify: none

**Step 1: Re-run tests**
- Run: `python3 -m unittest warehouse.tests.test_seed_ads_from_csv warehouse.tests.test_init_metabase_dashboard`

**Step 2: Reseed ADS tables**
- Run: `python3 warehouse/scripts/seed_ads_from_csv.py --force`

**Step 3: Re-sync dashboard**
- Run: `python3 warehouse/scripts/init_metabase_dashboard.py`

**Step 4: Validate UI**
- Confirm MySQL rows display correct Chinese.
- Confirm the dashboard uses full-width layout in Metabase.
