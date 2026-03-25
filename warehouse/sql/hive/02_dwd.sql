USE wenyu_dwd;

DROP TABLE IF EXISTS dwd_scenic_detail;
CREATE TABLE dwd_scenic_detail STORED AS PARQUET AS
SELECT
    name,
    level,
    CASE
        WHEN region IS NULL OR TRIM(region) = '' OR region = '\\N' THEN '未知'
        ELSE TRIM(region)
    END AS region_std,
    address,
    price,
    COALESCE(price_min, CAST(0 AS DECIMAL(10,2))) AS price_min,
    COALESCE(price_max, COALESCE(price_min, CAST(0 AS DECIMAL(10,2)))) AS price_max,
    CASE WHEN COALESCE(price_min, 0) = 0 THEN '免费/低价' WHEN price_min <= 50 THEN '50元内' ELSE '50元以上' END AS price_level,
    open_time,
    visit_duration,
    CASE
        WHEN best_visit_time IS NULL OR TRIM(best_visit_time) = '' OR best_visit_time = '\\N' THEN '未知时间'
        ELSE TRIM(best_visit_time)
    END AS best_visit_time,
    crawl_time AS etl_time,
    source_url,
    source_site
FROM wenyu_ods.ods_scenic_info;

DROP TABLE IF EXISTS dwd_show_detail;
CREATE TABLE dwd_show_detail STORED AS PARQUET AS
SELECT
    name,
    show_time,
    venue,
    CASE
        WHEN region IS NULL OR TRIM(region) = '' OR region = '\\N' OR region = '北京市' THEN '北京市'
        ELSE TRIM(region)
    END AS region_std,
    price_range,
    COALESCE(price_min, CAST(0 AS DECIMAL(10,2))) AS price_min,
    COALESCE(price_max, COALESCE(price_min, CAST(0 AS DECIMAL(10,2)))) AS price_max,
    CASE
        WHEN status IS NULL OR TRIM(status) = '' OR status = '\\N' THEN '待定'
        WHEN status IN ('售票中', '预售中', '已结束') THEN status
        ELSE TRIM(status)
    END AS status_std,
    COALESCE(attention, CAST(0 AS DECIMAL(10,2))) AS attention_num,
    crawl_time AS etl_time,
    source_url,
    source_site
FROM wenyu_ods.ods_show_info;

DROP TABLE IF EXISTS dwd_ktv_detail;
CREATE TABLE dwd_ktv_detail STORED AS PARQUET AS
SELECT
    name,
    CASE
        WHEN region IS NULL OR TRIM(region) = '' OR region = '\\N' THEN '未知'
        ELSE TRIM(region)
    END AS region_std,
    address,
    COALESCE(avg_cost, CAST(0 AS DECIMAL(10,2))) AS avg_cost,
    COALESCE(service_score, CAST(0 AS DECIMAL(10,2))) AS service_score,
    COALESCE(env_score, CAST(0 AS DECIMAL(10,2))) AS env_score,
    COALESCE(overall_score, CAST(0 AS DECIMAL(10,2))) AS overall_score,
    COALESCE(popularity, 0) AS popularity_num,
    business_hours,
    CASE
        WHEN COALESCE(avg_cost, 0) = 0 THEN CAST(0 AS DECIMAL(10,2))
        ELSE ROUND(COALESCE(overall_score, 0) / avg_cost, 4)
    END AS cost_performance,
    crawl_time AS etl_time,
    source_url,
    source_site
FROM wenyu_ods.ods_ktv_info;

DROP TABLE IF EXISTS dwd_movie_detail;
CREATE TABLE dwd_movie_detail STORED AS PARQUET AS
SELECT
    name,
    COALESCE(score, CAST(0 AS DECIMAL(10,2))) AS score_num,
    category,
    country_region,
    director,
    actors,
    intro,
    CASE
        WHEN COALESCE(score, 0) >= 9 THEN '9分及以上'
        WHEN COALESCE(score, 0) >= 8 THEN '8-9分'
        WHEN COALESCE(score, 0) >= 7 THEN '7-8分'
        WHEN COALESCE(score, 0) > 0 THEN '7分以下'
        ELSE '暂无评分'
    END AS score_level,
    crawl_time AS etl_time,
    source_url,
    source_site
FROM wenyu_ods.ods_movie_info;

DROP TABLE IF EXISTS dwd_sport_detail;
CREATE TABLE dwd_sport_detail STORED AS PARQUET AS
SELECT
    name,
    CASE
        WHEN venue_type IS NULL OR TRIM(venue_type) = '' OR venue_type = '\\N' THEN '其他场馆'
        ELSE TRIM(venue_type)
    END AS venue_type_std,
    CASE
        WHEN region IS NULL OR TRIM(region) = '' OR region = '\\N' THEN '未知'
        ELSE TRIM(region)
    END AS region_std,
    address,
    COALESCE(score, CAST(0 AS DECIMAL(10,2))) AS score_num,
    COALESCE(comment_count, 0) AS comment_count_num,
    COALESCE(avg_cost, CAST(0 AS DECIMAL(10,2))) AS avg_cost,
    open_time,
    crawl_time AS etl_time,
    source_url,
    source_site
FROM wenyu_ods.ods_sport_info;
