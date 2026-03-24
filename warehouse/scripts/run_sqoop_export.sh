#!/bin/bash
set -e

MYSQL_HOST=${MYSQL_HOST:-localhost}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_DB=${MYSQL_DB:-wenyu_result}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-root}

export_table() {
  local table_name=$1
  local export_dir=$2
  local update_key=$3

  sqoop export \
    --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai" \
    --username "$MYSQL_USER" \
    --password "$MYSQL_PASSWORD" \
    --table "$table_name" \
    --export-dir "$export_dir" \
    --update-mode allowinsert \
    --update-key "$update_key" \
    --fields-terminated-by '\t' \
    --input-null-string '\\N' \
    --input-null-non-string '\\N'
}

export_table ads_region_entertainment_count /user/hive/warehouse/wenyu_ads.db/ads_region_entertainment_count region
export_table ads_movie_score_distribution /user/hive/warehouse/wenyu_ads.db/ads_movie_score_distribution score_level
export_table ads_show_price_top10 /user/hive/warehouse/wenyu_ads.db/ads_show_price_top10 name
export_table ads_show_status_ratio /user/hive/warehouse/wenyu_ads.db/ads_show_status_ratio status_std
export_table ads_ktv_region_hotspot /user/hive/warehouse/wenyu_ads.db/ads_ktv_region_hotspot region
export_table ads_ktv_cost_performance_top5 /user/hive/warehouse/wenyu_ads.db/ads_ktv_cost_performance_top5 name
export_table ads_sport_type_ratio_top5 /user/hive/warehouse/wenyu_ads.db/ads_sport_type_ratio_top5 venue_type
export_table ads_scenic_free_ratio /user/hive/warehouse/wenyu_ads.db/ads_scenic_free_ratio scenic_type
