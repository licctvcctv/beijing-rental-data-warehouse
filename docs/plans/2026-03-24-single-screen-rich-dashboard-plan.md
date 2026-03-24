# Single Screen Rich Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Redesign the Metabase BI dashboard into a richer single-screen layout with more chart variety and no vertical scrolling on a typical desktop display.

**Architecture:** Replace the current evenly-stacked grid with a cockpit-style dashboard: KPI cards on top, major charts in the middle, supporting charts below. Expand the card set with scalar and table displays while keeping the dashboard within a compact row budget and syncing it through the existing Metabase API bootstrap script.

**Tech Stack:** Python 3, Metabase REST API, unittest

### Task 1: Add failing layout/design regression tests

**Files:**
- Modify: `warehouse/tests/test_init_metabase_dashboard.py`

**Step 1: Write a failing test for display variety**
- Assert the configured cards include `scalar`, `table`, `bar`, and `pie`.

**Step 2: Write a failing test for compact height**
- Assert the maximum `row + size_y` stays within a one-screen budget.

**Step 3: Run tests to verify they fail**
- Run: `python3 -m unittest warehouse.tests.test_init_metabase_dashboard`

### Task 2: Redesign dashboard card specs

**Files:**
- Modify: `warehouse/scripts/init_metabase_dashboard.py`

**Step 1: Replace current cards with a cockpit layout**
- Add 4 KPI cards.
- Keep 2-3 main charts.
- Keep 3-4 supporting charts.
- Add at least one table card.

**Step 2: Keep dashboard width full**
- Preserve the `width: full` setting and sync the new compact layout.

### Task 3: Verify live Metabase output

**Files:**
- Modify: none

**Step 1: Run regression tests**
- Run: `python3 -m unittest warehouse.tests.test_init_metabase_dashboard warehouse.tests.test_seed_ads_from_csv`

**Step 2: Re-sync dashboard**
- Run: `python3 warehouse/scripts/init_metabase_dashboard.py`

**Step 3: Confirm dashboard metadata**
- Verify the dashboard still has `width: full`.
- Verify the dashboard contains the richer card set and compact layout.
