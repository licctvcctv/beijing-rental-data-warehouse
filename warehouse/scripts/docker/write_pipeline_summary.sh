#!/bin/bash
set -euo pipefail

OUTPUT_DIR=/workspace/warehouse/target/docker-hadoop-output
SUMMARY_FILE="${OUTPUT_DIR}/summary.txt"
MYSQL_COUNTS_FILE="${OUTPUT_DIR}/mysql_counts.tsv"
HIVE_SAMPLE_FILE="${OUTPUT_DIR}/ads_xzq_avg_rent.tsv"

mkdir -p "${OUTPUT_DIR}"

mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u "${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D "${MYSQL_DB:?MYSQL_DB not set}" -N -B <<'SQL' > "${MYSQL_COUNTS_FILE}"
SELECT 'ads_xzq_avg_rent', COUNT(*) FROM ads_xzq_avg_rent
UNION ALL
SELECT 'ads_fy_heatmap', COUNT(*) FROM ads_fy_heatmap
UNION ALL
SELECT 'ads_sq_top10', COUNT(*) FROM ads_sq_top10
UNION ALL
SELECT 'ads_fy_type_ratio', COUNT(*) FROM ads_fy_type_ratio
UNION ALL
SELECT 'ads_price_area_scatter', COUNT(*) FROM ads_price_area_scatter
UNION ALL
SELECT 'ads_metro_rent_compare', COUNT(*) FROM ads_metro_rent_compare
UNION ALL
SELECT 'ads_zx_avg_rent', COUNT(*) FROM ads_zx_avg_rent
UNION ALL
SELECT 'ads_platform_distribution', COUNT(*) FROM ads_platform_distribution;
SQL

hive -S -e "USE rental_ads; SELECT xzq, pj_zj, fysl FROM ads_xzq_avg_rent ORDER BY pj_zj DESC;" > "${HIVE_SAMPLE_FILE}"

{
  echo "pipeline_status=SUCCESS"
  echo "run_time=$(date '+%F %T %z')"
  echo "hdfs_uri=hdfs://$(hostname):9820"
  echo
  echo "[hdfs_paths]"
  hdfs dfs -ls /data || true
  hdfs dfs -ls /data/logs || true
  hdfs dfs -ls /data/clean || true
  hdfs dfs -ls /user/hive/warehouse/rental_ads.db || true
  echo
  echo "[mysql_counts]"
  cat "${MYSQL_COUNTS_FILE}"
} > "${SUMMARY_FILE}"
