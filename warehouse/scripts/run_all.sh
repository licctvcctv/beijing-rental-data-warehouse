#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")" && pwd)

log() {
  echo "[pipeline] $1"
}

print_ads_counts() {
  hive -S <<'SQL'
USE wenyu_ads;
SELECT 'ads_region_entertainment_count', COUNT(1) FROM ads_region_entertainment_count
UNION ALL
SELECT 'ads_movie_score_distribution', COUNT(1) FROM ads_movie_score_distribution
UNION ALL
SELECT 'ads_show_price_top10', COUNT(1) FROM ads_show_price_top10
UNION ALL
SELECT 'ads_show_status_ratio', COUNT(1) FROM ads_show_status_ratio
UNION ALL
SELECT 'ads_ktv_region_hotspot', COUNT(1) FROM ads_ktv_region_hotspot
UNION ALL
SELECT 'ads_ktv_cost_performance_top5', COUNT(1) FROM ads_ktv_cost_performance_top5
UNION ALL
SELECT 'ads_sport_type_ratio_top5', COUNT(1) FROM ads_sport_type_ratio_top5
UNION ALL
SELECT 'ads_scenic_free_ratio', COUNT(1) FROM ads_scenic_free_ratio;
SQL
}

log "Stage 1/6: upload raw CSV to HDFS"
"$BASE_DIR/run_upload.sh"

log "Stage 2/6: run Java MapReduce cleaning jobs"
"$BASE_DIR/run_mr_clean.sh"

log "Stage 3/6: build ODS layer in Hive"
"$BASE_DIR/run_hive_ods.sh"

log "Stage 4/6: build DWD layer in Hive"
"$BASE_DIR/run_hive_dwd.sh"

log "Stage 5/6: build DWS and ADS layers in Hive"
"$BASE_DIR/run_hive_dws_ads.sh"

log "ADS row counts before Sqoop export"
print_ads_counts

log "Stage 6/6: export ADS tables to MySQL with Sqoop"
"$BASE_DIR/run_sqoop_export.sh"
