# BI Demo Seed Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the BI dashboard show data even when the full Hadoop/Hive/Sqoop pipeline has not been executed.

**Architecture:** Add a lightweight fallback script that reads the existing raw CSV exports, computes the eight ADS result tables required by Metabase, and writes those rows directly into MySQL. Hook the BI startup flow to call this fallback only when the ADS tables are empty, so real warehouse exports can still take precedence later.

**Tech Stack:** Python 3, CSV standard library, MySQL container CLI, shell scripts, unittest

### Task 1: Cover fallback aggregation with tests

**Files:**
- Create: `warehouse/tests/test_seed_ads_from_csv.py`
- Modify: none

**Step 1: Write failing tests**

- Assert region entertainment counts are produced from scenic/show/ktv/sport rows.
- Assert scenic free/paid ratio and movie score bins are computed from raw values.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest warehouse.tests.test_seed_ads_from_csv`

Expected: FAIL because the seeding module does not exist yet.

### Task 2: Implement CSV aggregation and MySQL seeding

**Files:**
- Create: `warehouse/scripts/seed_ads_from_csv.py`
- Test: `warehouse/tests/test_seed_ads_from_csv.py`

**Step 1: Write minimal implementation**

- Read `crawler/data/export/*.csv`.
- Normalize district names from `region` or `address`.
- Compute the eight ADS result sets needed by Metabase.
- Provide a CLI that inserts rows into MySQL with `REPLACE INTO`.

**Step 2: Run targeted tests**

Run: `python3 -m unittest warehouse.tests.test_seed_ads_from_csv`

Expected: PASS

### Task 3: Hook fallback into BI startup and verify live dashboard data

**Files:**
- Modify: `warehouse/scripts/run_bi_stack.sh`
- Modify: `warehouse/scripts/run_all_with_bi.sh`
- Test: `warehouse/tests/test_seed_ads_from_csv.py`

**Step 1: Add fallback trigger**

- After MySQL starts and before Metabase init, invoke the seeding script.
- Make the script skip writes when any ADS tables already contain data.

**Step 2: Verify end to end**

Run:
- `python3 -m unittest warehouse.tests.test_seed_ads_from_csv warehouse.tests.test_init_metabase_dashboard`
- `python3 warehouse/scripts/seed_ads_from_csv.py`
- SQL row-count checks against `wenyu_result`
- Metabase dashboard verification

Expected: ADS tables non-empty and dashboard charts show data after refresh.
