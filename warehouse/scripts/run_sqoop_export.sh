#!/bin/bash
set -e

MYSQL_HOST=${MYSQL_HOST:-localhost}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_DB=${MYSQL_DB:-rental_result}
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

export_table ads_xzq_avg_rent /user/hive/warehouse/rental_ads.db/ads_xzq_avg_rent xzq,pj_zj,fysl,max_zj,min_zj
export_table ads_fy_heatmap /user/hive/warehouse/rental_ads.db/ads_fy_heatmap sq,xzq,pj_zj,center_jd,center_wd,fysl
export_table ads_sq_top10 /user/hive/warehouse/rental_ads.db/ads_sq_top10 sq,xzq,fysl,pj_zj
export_table ads_fy_type_ratio /user/hive/warehouse/rental_ads.db/ads_fy_type_ratio fy_type,fysl,type_ratio,pj_zj,pj_mj
export_table ads_price_area_scatter /user/hive/warehouse/rental_ads.db/ads_price_area_scatter fy_id,xzq,unit_dj,jzmj,month_zj
export_table ads_metro_rent_compare /user/hive/warehouse/rental_ads.db/ads_metro_rent_compare xzq,is_dt,fysl,pj_zj
export_table ads_zx_avg_rent /user/hive/warehouse/rental_ads.db/ads_zx_avg_rent zx_qk,fysl,pj_zj,pj_dj
export_table ads_platform_distribution /user/hive/warehouse/rental_ads.db/ads_platform_distribution platform,fysl,pj_zj
