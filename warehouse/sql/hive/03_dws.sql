USE wenyu_dws;

DROP TABLE IF EXISTS dws_region_summary;
CREATE TABLE dws_region_summary STORED AS PARQUET AS
SELECT region_std AS region, 'scenic' AS category, COUNT(1) AS total_count FROM wenyu_dwd.dwd_scenic_detail GROUP BY region_std
UNION ALL
SELECT region_std AS region, 'show' AS category, COUNT(1) AS total_count FROM wenyu_dwd.dwd_show_detail GROUP BY region_std
UNION ALL
SELECT region_std AS region, 'ktv' AS category, COUNT(1) AS total_count FROM wenyu_dwd.dwd_ktv_detail GROUP BY region_std
UNION ALL
SELECT '北京市' AS region, 'movie' AS category, COUNT(1) AS total_count FROM wenyu_dwd.dwd_movie_detail
UNION ALL
SELECT region_std AS region, 'sport' AS category, COUNT(1) AS total_count FROM wenyu_dwd.dwd_sport_detail GROUP BY region_std;

DROP TABLE IF EXISTS dws_movie_score_summary;
CREATE TABLE dws_movie_score_summary STORED AS PARQUET AS
SELECT score_level, COUNT(1) AS movie_count, ROUND(AVG(score_num), 2) AS avg_score
FROM wenyu_dwd.dwd_movie_detail
GROUP BY score_level;

DROP TABLE IF EXISTS dws_show_status_summary;
CREATE TABLE dws_show_status_summary STORED AS PARQUET AS
SELECT status_std, COUNT(1) AS show_count, ROUND(AVG(attention_num), 2) AS avg_attention
FROM wenyu_dwd.dwd_show_detail
GROUP BY status_std;

DROP TABLE IF EXISTS dws_show_price_summary;
CREATE TABLE dws_show_price_summary STORED AS PARQUET AS
SELECT name, venue, region_std, price_min, price_max, status_std, attention_num
FROM wenyu_dwd.dwd_show_detail;

DROP TABLE IF EXISTS dws_ktv_region_summary;
CREATE TABLE dws_ktv_region_summary STORED AS PARQUET AS
SELECT region_std, COUNT(1) AS ktv_count, ROUND(AVG(avg_cost), 2) AS avg_cost, ROUND(AVG(overall_score), 2) AS avg_score
FROM wenyu_dwd.dwd_ktv_detail
GROUP BY region_std;

DROP TABLE IF EXISTS dws_sport_type_summary;
CREATE TABLE dws_sport_type_summary STORED AS PARQUET AS
SELECT venue_type_std, COUNT(1) AS venue_count, ROUND(AVG(score_num), 2) AS avg_score
FROM wenyu_dwd.dwd_sport_detail
GROUP BY venue_type_std;

DROP TABLE IF EXISTS dws_scenic_visit_time_summary;
CREATE TABLE dws_scenic_visit_time_summary STORED AS PARQUET AS
SELECT best_visit_time, COUNT(1) AS scenic_count,
       SUM(CASE WHEN price_min = 0 THEN 1 ELSE 0 END) AS free_count
FROM wenyu_dwd.dwd_scenic_detail
GROUP BY best_visit_time;
