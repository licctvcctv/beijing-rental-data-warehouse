CREATE DATABASE IF NOT EXISTS `wenyu_result` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `wenyu_result`;

DROP TABLE IF EXISTS ads_region_entertainment_count;
CREATE TABLE ads_region_entertainment_count (
    region VARCHAR(64) NOT NULL,
    entertainment_count BIGINT DEFAULT 0,
    PRIMARY KEY (region)
) COMMENT='各区娱乐资源总量';

DROP TABLE IF EXISTS ads_movie_score_distribution;
CREATE TABLE ads_movie_score_distribution (
    score_level VARCHAR(64) NOT NULL,
    movie_count BIGINT DEFAULT 0,
    avg_score DECIMAL(10,2) DEFAULT NULL,
    PRIMARY KEY (score_level)
) COMMENT='电影评分分布';

DROP TABLE IF EXISTS ads_show_price_top10;
CREATE TABLE ads_show_price_top10 (
    name VARCHAR(255) NOT NULL,
    venue VARCHAR(255) DEFAULT NULL,
    region VARCHAR(64) DEFAULT NULL,
    price_max DECIMAL(10,2) DEFAULT NULL,
    price_min DECIMAL(10,2) DEFAULT NULL,
    status_std VARCHAR(64) DEFAULT NULL,
    attention_num DECIMAL(10,2) DEFAULT NULL,
    PRIMARY KEY (name)
) COMMENT='演出价格Top10';

DROP TABLE IF EXISTS ads_show_status_ratio;
CREATE TABLE ads_show_status_ratio (
    status_std VARCHAR(64) NOT NULL,
    show_count BIGINT DEFAULT 0,
    status_ratio DECIMAL(10,4) DEFAULT NULL,
    PRIMARY KEY (status_std)
) COMMENT='演出售票状态占比';

DROP TABLE IF EXISTS ads_ktv_region_hotspot;
CREATE TABLE ads_ktv_region_hotspot (
    region VARCHAR(64) NOT NULL,
    ktv_count BIGINT DEFAULT 0,
    avg_cost DECIMAL(10,2) DEFAULT NULL,
    avg_score DECIMAL(10,2) DEFAULT NULL,
    PRIMARY KEY (region)
) COMMENT='KTV区域热度';

DROP TABLE IF EXISTS ads_ktv_cost_performance_top5;
CREATE TABLE ads_ktv_cost_performance_top5 (
    name VARCHAR(255) NOT NULL,
    region VARCHAR(64) DEFAULT NULL,
    avg_cost DECIMAL(10,2) DEFAULT NULL,
    overall_score DECIMAL(10,2) DEFAULT NULL,
    cost_performance DECIMAL(10,4) DEFAULT NULL,
    popularity_num BIGINT DEFAULT 0,
    PRIMARY KEY (name)
) COMMENT='KTV性价比Top5';

DROP TABLE IF EXISTS ads_sport_type_ratio_top5;
CREATE TABLE ads_sport_type_ratio_top5 (
    venue_type VARCHAR(64) NOT NULL,
    venue_count BIGINT DEFAULT 0,
    venue_ratio DECIMAL(10,4) DEFAULT NULL,
    avg_score DECIMAL(10,2) DEFAULT NULL,
    PRIMARY KEY (venue_type)
) COMMENT='体育场馆类型Top5';

DROP TABLE IF EXISTS ads_scenic_free_ratio;
CREATE TABLE ads_scenic_free_ratio (
    scenic_type VARCHAR(64) NOT NULL,
    scenic_count BIGINT DEFAULT 0,
    scenic_ratio DECIMAL(10,4) DEFAULT NULL,
    PRIMARY KEY (scenic_type)
) COMMENT='景点免费占比';
