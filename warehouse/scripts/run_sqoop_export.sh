#!/bin/bash
set -e

MYSQL_HOST=${MYSQL_HOST:-localhost}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_DB=${MYSQL_DB:-wenyu_result}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-root}
MYSQL_JDBC_URL="jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true"

truncate_table() {
  local table_name=$1
  mysql \
    -h "$MYSQL_HOST" \
    -P "$MYSQL_PORT" \
    -u "$MYSQL_USER" \
    -p"$MYSQL_PASSWORD" \
    -D "$MYSQL_DB" \
    -e "TRUNCATE TABLE \`${table_name}\`;"
}

export_table() {
  local table_name=$1
  local export_dir=$2
  local columns=$3

  truncate_table "$table_name"

  sqoop export \
    --connect "$MYSQL_JDBC_URL" \
    --driver com.mysql.cj.jdbc.Driver \
    --username "$MYSQL_USER" \
    --password "$MYSQL_PASSWORD" \
    --table "$table_name" \
    --columns "$columns" \
    --export-dir "$export_dir" \
    --num-mappers 1 \
    --input-fields-terminated-by '\t' \
    --input-null-string '\\N' \
    --input-null-non-string '\\N'
}

export_table ads_region_entertainment_count /user/hive/warehouse/wenyu_ads.db/ads_region_entertainment_count region,entertainment_count
export_table ads_movie_score_distribution /user/hive/warehouse/wenyu_ads.db/ads_movie_score_distribution score_level,movie_count,avg_score
export_table ads_show_price_top10 /user/hive/warehouse/wenyu_ads.db/ads_show_price_top10 name,venue,region,price_max,price_min,status_std,attention_num
export_table ads_show_status_ratio /user/hive/warehouse/wenyu_ads.db/ads_show_status_ratio status_std,show_count,status_ratio
export_table ads_ktv_region_hotspot /user/hive/warehouse/wenyu_ads.db/ads_ktv_region_hotspot region,ktv_count,avg_cost,avg_score
export_table ads_ktv_cost_performance_top5 /user/hive/warehouse/wenyu_ads.db/ads_ktv_cost_performance_top5 name,region,avg_cost,overall_score,cost_performance,popularity_num
export_table ads_sport_type_ratio_top5 /user/hive/warehouse/wenyu_ads.db/ads_sport_type_ratio_top5 venue_type,venue_count,venue_ratio,avg_score
export_table ads_scenic_free_ratio /user/hive/warehouse/wenyu_ads.db/ads_scenic_free_ratio scenic_type,scenic_count,scenic_ratio
