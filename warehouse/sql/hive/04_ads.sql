USE rental_ads;

-- 1. 各行政区平均租金（柱状图）
DROP TABLE IF EXISTS ads_xzq_avg_rent;
CREATE TABLE ads_xzq_avg_rent
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT xzq, pj_zj, fysl, max_zj, min_zj
FROM rental_dws.dws_fy_xzq_zj;

-- 2. 租金热力图（商圈维度）
DROP TABLE IF EXISTS ads_fy_heatmap;
CREATE TABLE ads_fy_heatmap
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT sq, xzq, pj_zj, center_jd, center_wd, fysl
FROM rental_dws.dws_fy_sq_summary;

-- 3. 商圈房源数量 TOP10（折线图）
DROP TABLE IF EXISTS ads_sq_top10;
CREATE TABLE ads_sq_top10
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT sq, xzq, fysl, pj_zj
FROM rental_dws.dws_fy_sq_summary
ORDER BY fysl DESC
LIMIT 10;

-- 4. 房源类型占比（饼图）
DROP TABLE IF EXISTS ads_fy_type_ratio;
CREATE TABLE ads_fy_type_ratio
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
    fy_type,
    fysl,
    ROUND(fysl / SUM(fysl) OVER (), 4) AS type_ratio,
    pj_zj,
    pj_mj
FROM rental_dws.dws_fy_type_summary;

-- 5. 租金单价与面积散点（散点图数据来自DWD明细）
DROP TABLE IF EXISTS ads_price_area_scatter;
CREATE TABLE ads_price_area_scatter
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT fy_id, xzq, unit_dj, jzmj, month_zj
FROM rental_dwd.dwd_fy_mx
WHERE jzmj > 0 AND unit_dj > 0;

-- 6. 地铁房 vs 非地铁房租金对比（分组柱状图）
DROP TABLE IF EXISTS ads_metro_rent_compare;
CREATE TABLE ads_metro_rent_compare
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT xzq, is_dt, fysl, pj_zj
FROM rental_dws.dws_fy_metro_compare;

-- 7. 各装修情况平均租金（雷达图）
DROP TABLE IF EXISTS ads_zx_avg_rent;
CREATE TABLE ads_zx_avg_rent
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT zx_qk, fysl, pj_zj, pj_dj
FROM rental_dws.dws_fy_zx_summary;

-- 8. 各平台房源分布（条形图）
DROP TABLE IF EXISTS ads_platform_distribution;
CREATE TABLE ads_platform_distribution
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT platform, fysl, pj_zj
FROM rental_dws.dws_fy_platform_summary;
