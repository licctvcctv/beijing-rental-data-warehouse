#!/bin/bash
set -euo pipefail

OUTPUT_DIR=/workspace/warehouse/target/docker-hadoop-output
SUMMARY_FILE="${OUTPUT_DIR}/summary.txt"
MYSQL_COUNTS_FILE="${OUTPUT_DIR}/mysql_counts.tsv"
HIVE_SAMPLE_FILE="${OUTPUT_DIR}/ads_region_entertainment_count.tsv"

mkdir -p "${OUTPUT_DIR}"

mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u "${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D "${MYSQL_DB:?MYSQL_DB not set}" -N -B <<'SQL' > "${MYSQL_COUNTS_FILE}"
SELECT 'ads_region_entertainment_count', COUNT(*) FROM ads_region_entertainment_count
UNION ALL
SELECT 'ads_movie_score_distribution', COUNT(*) FROM ads_movie_score_distribution
UNION ALL
SELECT 'ads_show_price_top10', COUNT(*) FROM ads_show_price_top10
UNION ALL
SELECT 'ads_show_status_ratio', COUNT(*) FROM ads_show_status_ratio
UNION ALL
SELECT 'ads_ktv_region_hotspot', COUNT(*) FROM ads_ktv_region_hotspot
UNION ALL
SELECT 'ads_ktv_cost_performance_top5', COUNT(*) FROM ads_ktv_cost_performance_top5
UNION ALL
SELECT 'ads_sport_type_ratio_top5', COUNT(*) FROM ads_sport_type_ratio_top5
UNION ALL
SELECT 'ads_scenic_free_ratio', COUNT(*) FROM ads_scenic_free_ratio;
SQL

hive -S -e "USE wenyu_ads; SELECT region, entertainment_count FROM ads_region_entertainment_count ORDER BY entertainment_count DESC;" > "${HIVE_SAMPLE_FILE}"

{
  echo "pipeline_status=SUCCESS"
  echo "run_time=$(date '+%F %T %z')"
  echo "hdfs_uri=hdfs://$(hostname):9820"
  echo
  echo "[hdfs_paths]"
  hdfs dfs -ls /data || true
  hdfs dfs -ls /data/logs || true
  hdfs dfs -ls /data/clean || true
  hdfs dfs -ls /user/hive/warehouse/wenyu_ads.db || true
  echo
  echo "[mysql_counts]"
  cat "${MYSQL_COUNTS_FILE}"
} > "${SUMMARY_FILE}"
