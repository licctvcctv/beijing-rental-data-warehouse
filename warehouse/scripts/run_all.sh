#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")" && pwd)

log() {
  echo "[pipeline] $1"
}

print_ads_counts() {
  hive -S <<'SQL'
USE rental_ads;
SELECT 'ads_xzq_avg_rent', COUNT(1) FROM ads_xzq_avg_rent
UNION ALL
SELECT 'ads_fy_heatmap', COUNT(1) FROM ads_fy_heatmap
UNION ALL
SELECT 'ads_sq_top10', COUNT(1) FROM ads_sq_top10
UNION ALL
SELECT 'ads_fy_type_ratio', COUNT(1) FROM ads_fy_type_ratio
UNION ALL
SELECT 'ads_price_area_scatter', COUNT(1) FROM ads_price_area_scatter
UNION ALL
SELECT 'ads_metro_rent_compare', COUNT(1) FROM ads_metro_rent_compare
UNION ALL
SELECT 'ads_zx_avg_rent', COUNT(1) FROM ads_zx_avg_rent
UNION ALL
SELECT 'ads_platform_distribution', COUNT(1) FROM ads_platform_distribution;
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
