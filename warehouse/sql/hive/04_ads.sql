USE wenyu_ads;

DROP TABLE IF EXISTS ads_region_entertainment_count;
CREATE TABLE ads_region_entertainment_count
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
    CASE WHEN region IS NULL OR TRIM(region) = '' THEN '未知' ELSE region END AS region,
    SUM(total_count) AS entertainment_count
FROM wenyu_dws.dws_region_summary
GROUP BY CASE WHEN region IS NULL OR TRIM(region) = '' THEN '未知' ELSE region END;

DROP TABLE IF EXISTS ads_movie_score_distribution;
CREATE TABLE ads_movie_score_distribution
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
    CASE WHEN score_level IS NULL OR TRIM(score_level) = '' THEN '未分级' ELSE score_level END AS score_level,
    movie_count,
    avg_score
FROM wenyu_dws.dws_movie_score_summary;

DROP TABLE IF EXISTS ads_show_price_top10;
CREATE TABLE ads_show_price_top10
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
    name,
    venue,
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '北京市' ELSE region_std END AS region,
    price_max,
    price_min,
    CASE WHEN status_std IS NULL OR TRIM(status_std) = '' THEN '待定' ELSE status_std END AS status_std,
    attention_num
FROM wenyu_dws.dws_show_price_summary
ORDER BY price_max DESC, attention_num DESC
LIMIT 10;

DROP TABLE IF EXISTS ads_show_status_ratio;
CREATE TABLE ads_show_status_ratio
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
       CASE WHEN status_std IS NULL OR TRIM(status_std) = '' THEN '待定' ELSE status_std END AS status_std,
       show_count,
       ROUND(show_count / SUM(show_count) OVER (), 4) AS status_ratio
FROM wenyu_dws.dws_show_status_summary;

DROP TABLE IF EXISTS ads_ktv_region_hotspot;
CREATE TABLE ads_ktv_region_hotspot
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END AS region,
    ktv_count,
    avg_cost,
    avg_score
FROM wenyu_dws.dws_ktv_region_summary
ORDER BY ktv_count DESC, avg_score DESC;

DROP TABLE IF EXISTS ads_ktv_cost_performance_top5;
CREATE TABLE ads_ktv_cost_performance_top5
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
    name,
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END AS region,
    avg_cost,
    overall_score,
    cost_performance,
    popularity_num
FROM wenyu_dwd.dwd_ktv_detail
WHERE avg_cost > 0
ORDER BY cost_performance DESC, popularity_num DESC
LIMIT 5;

DROP TABLE IF EXISTS ads_sport_type_ratio_top5;
CREATE TABLE ads_sport_type_ratio_top5
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT
       CASE WHEN venue_type_std IS NULL OR TRIM(venue_type_std) = '' THEN '其他场馆' ELSE venue_type_std END AS venue_type,
       venue_count,
       ROUND(venue_count / SUM(venue_count) OVER (), 4) AS venue_ratio,
       avg_score
FROM wenyu_dws.dws_sport_type_summary
ORDER BY venue_count DESC
LIMIT 5;

DROP TABLE IF EXISTS ads_scenic_free_ratio;
CREATE TABLE ads_scenic_free_ratio
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE AS
SELECT scenic_type, scenic_count, ROUND(scenic_count / total, 4) AS scenic_ratio
FROM (
    SELECT '免费景点' AS scenic_type,
           SUM(free_count) AS scenic_count,
           SUM(scenic_count) AS total
    FROM wenyu_dws.dws_scenic_visit_time_summary
    UNION ALL
    SELECT '收费景点' AS scenic_type,
           SUM(scenic_count) - SUM(free_count) AS scenic_count,
           SUM(scenic_count) AS total
    FROM wenyu_dws.dws_scenic_visit_time_summary
) t
WHERE total > 0;
