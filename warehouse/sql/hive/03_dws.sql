USE wenyu_dws;

DROP TABLE IF EXISTS dws_region_summary;
CREATE TABLE dws_region_summary STORED AS PARQUET AS
SELECT
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END AS region,
    'scenic' AS category,
    COUNT(1) AS total_count
FROM wenyu_dwd.dwd_scenic_detail
GROUP BY CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END
UNION ALL
SELECT
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '北京市' ELSE region_std END AS region,
    'show' AS category,
    COUNT(1) AS total_count
FROM wenyu_dwd.dwd_show_detail
GROUP BY CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '北京市' ELSE region_std END
UNION ALL
SELECT
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END AS region,
    'ktv' AS category,
    COUNT(1) AS total_count
FROM wenyu_dwd.dwd_ktv_detail
GROUP BY CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END
UNION ALL
SELECT '北京市' AS region, 'movie' AS category, COUNT(1) AS total_count FROM wenyu_dwd.dwd_movie_detail
UNION ALL
SELECT
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END AS region,
    'sport' AS category,
    COUNT(1) AS total_count
FROM wenyu_dwd.dwd_sport_detail
GROUP BY CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END;

DROP TABLE IF EXISTS dws_movie_score_summary;
CREATE TABLE dws_movie_score_summary STORED AS PARQUET AS
SELECT
    CASE WHEN score_level IS NULL OR TRIM(score_level) = '' THEN '未分级' ELSE score_level END AS score_level,
    COUNT(1) AS movie_count,
    ROUND(AVG(score_num), 2) AS avg_score
FROM wenyu_dwd.dwd_movie_detail
GROUP BY CASE WHEN score_level IS NULL OR TRIM(score_level) = '' THEN '未分级' ELSE score_level END;

DROP TABLE IF EXISTS dws_show_status_summary;
CREATE TABLE dws_show_status_summary STORED AS PARQUET AS
SELECT
    CASE WHEN status_std IS NULL OR TRIM(status_std) = '' THEN '待定' ELSE status_std END AS status_std,
    COUNT(1) AS show_count,
    ROUND(AVG(attention_num), 2) AS avg_attention
FROM wenyu_dwd.dwd_show_detail
GROUP BY CASE WHEN status_std IS NULL OR TRIM(status_std) = '' THEN '待定' ELSE status_std END;

DROP TABLE IF EXISTS dws_show_price_summary;
CREATE TABLE dws_show_price_summary STORED AS PARQUET AS
SELECT
    name,
    venue,
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '北京市' ELSE region_std END AS region_std,
    price_min,
    price_max,
    CASE WHEN status_std IS NULL OR TRIM(status_std) = '' THEN '待定' ELSE status_std END AS status_std,
    attention_num
FROM wenyu_dwd.dwd_show_detail;

DROP TABLE IF EXISTS dws_ktv_region_summary;
CREATE TABLE dws_ktv_region_summary STORED AS PARQUET AS
SELECT
    CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END AS region_std,
    COUNT(1) AS ktv_count,
    ROUND(AVG(avg_cost), 2) AS avg_cost,
    ROUND(AVG(overall_score), 2) AS avg_score
FROM wenyu_dwd.dwd_ktv_detail
GROUP BY CASE WHEN region_std IS NULL OR TRIM(region_std) = '' THEN '未知' ELSE region_std END;

DROP TABLE IF EXISTS dws_sport_type_summary;
CREATE TABLE dws_sport_type_summary STORED AS PARQUET AS
SELECT
    CASE WHEN venue_type_std IS NULL OR TRIM(venue_type_std) = '' THEN '其他场馆' ELSE venue_type_std END AS venue_type_std,
    COUNT(1) AS venue_count,
    ROUND(AVG(score_num), 2) AS avg_score
FROM wenyu_dwd.dwd_sport_detail
GROUP BY CASE WHEN venue_type_std IS NULL OR TRIM(venue_type_std) = '' THEN '其他场馆' ELSE venue_type_std END;

DROP TABLE IF EXISTS dws_scenic_visit_time_summary;
CREATE TABLE dws_scenic_visit_time_summary STORED AS PARQUET AS
SELECT
       CASE WHEN best_visit_time IS NULL OR TRIM(best_visit_time) = '' THEN '未知时间' ELSE best_visit_time END AS best_visit_time,
       COUNT(1) AS scenic_count,
       SUM(CASE WHEN price_min = 0 THEN 1 ELSE 0 END) AS free_count
FROM wenyu_dwd.dwd_scenic_detail
GROUP BY CASE WHEN best_visit_time IS NULL OR TRIM(best_visit_time) = '' THEN '未知时间' ELSE best_visit_time END;
