# Crawler Source Expansion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expand the crawler so BI can show substantially more Beijing entertainment data without degrading dashboard quality.

**Architecture:** Keep the existing category model, but widen two proven sources instead of inventing new schemas. `ktv` will merge the Cityhui `ktv` and `shangwuktv` lists with record-level deduplication. `show` will keep Dahe for ticket status and add the 东方演出网 Beijing city list for high-volume event coverage, while the ADS status chart will ignore unknown statuses introduced by the new source.

**Tech Stack:** Python, BeautifulSoup, unittest, existing CSV export flow, existing ADS fallback seeding script.

### Task 1: Lock in KTV expansion behavior

**Files:**
- Create: `crawler/tests/test_ktv_crawler.py`
- Modify: `crawler/src/ktv/crawler.py`

**Step 1: Write the failing test**

Add tests that prove:
- `discover_links()` loads both `https://www.cityhui.com/beijing/ktv/` and `https://www.cityhui.com/beijing/shangwuktv/`
- `parse_detail()` extracts a real Beijing district address from a business-KTV detail page
- `crawl()` removes duplicate business-KTV records that point to the same venue

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest crawler.tests.test_ktv_crawler -v`
Expected: FAIL because the current crawler only reads one list and does not dedupe business KTV pages.

**Step 3: Write minimal implementation**

Update `crawler/src/ktv/crawler.py` to:
- define both Cityhui list sources
- collect links from both pages
- improve label extraction for `地址` and `消费价格`
- dedupe parsed records by stable venue key before CSV write

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest crawler.tests.test_ktv_crawler -v`
Expected: PASS

### Task 2: Lock in show-source expansion behavior

**Files:**
- Create: `crawler/tests/test_show_crawler.py`
- Modify: `crawler/src/show/crawler.py`

**Step 1: Write the failing test**

Add tests that prove:
- the crawler still parses Dahe pages
- the crawler also parses 东方演出网 `https://www.df962388.com/yanchu/beijing/`
- the crawler respects a capped page budget for the new source
- duplicate events from multiple sources are merged down to one exported record

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest crawler.tests.test_show_crawler -v`
Expected: FAIL because the current crawler only knows the Dahe source.

**Step 3: Write minimal implementation**

Update `crawler/src/show/crawler.py` to:
- add 东方演出网 Beijing list pagination parsing
- build `ShowRecord` rows directly from list cards
- keep Dahe for ticket status
- merge and dedupe final records before writing `show_raw.csv`

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest crawler.tests.test_show_crawler -v`
Expected: PASS

### Task 3: Preserve dashboard quality with mixed show sources

**Files:**
- Modify: `warehouse/tests/test_seed_ads_from_csv.py`
- Modify: `warehouse/scripts/seed_ads_from_csv.py`

**Step 1: Write the failing test**

Add a test showing that rows whose `status` is missing should not overwhelm `ads_show_status_ratio` when explicit statuses still exist.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest warehouse.tests.test_seed_ads_from_csv -v`
Expected: FAIL because the current ADS logic counts unknown statuses in the ratio pie.

**Step 3: Write minimal implementation**

Update `warehouse/scripts/seed_ads_from_csv.py` so:
- explicit statuses are still normalized
- `ads_show_status_ratio` excludes `待定` when at least one explicit status exists

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest warehouse.tests.test_seed_ads_from_csv -v`
Expected: PASS

### Task 4: Docs, crawl, reseed, verify

**Files:**
- Modify: `crawler/docs/field_mapping.md`

**Step 1: Update docs**

Document the new KTV and show source coverage and any page caps.

**Step 2: Run targeted tests**

Run:
- `python3 -m unittest crawler.tests.test_ktv_crawler crawler.tests.test_show_crawler -v`
- `python3 -m unittest crawler.tests.test_cli crawler.tests.test_http crawler.tests.test_movie_crawler crawler.tests.test_scenic_crawler crawler.tests.test_sport_crawler warehouse.tests.test_seed_ads_from_csv -v`

Expected: PASS

**Step 3: Run real crawls and reseed**

Run:
- `python3 crawler/src/main.py ktv`
- `python3 crawler/src/main.py show`
- `python3 warehouse/scripts/seed_ads_from_csv.py --force`

Expected:
- `crawler/data/export/ktv_raw.csv` grows materially beyond the previous 11 rows
- `crawler/data/export/show_raw.csv` grows materially beyond the previous 48 rows
- ADS tables reseed successfully

**Step 4: Verify counts**

Run a short check that reports:
- raw CSV row counts
- `ads_region_entertainment_count` total
- show top-10 and status-ratio sanity

Expected: data totals increase and the dashboard remains readable.
