CREATE DATABASE IF NOT EXISTS `rental_result` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `rental_result`;

DROP TABLE IF EXISTS ads_xzq_avg_rent;
CREATE TABLE ads_xzq_avg_rent (
    xzq VARCHAR(64) NOT NULL,
    pj_zj DECIMAL(7,2) DEFAULT NULL COMMENT '平均租金',
    fysl BIGINT DEFAULT 0 COMMENT '房源数量',
    max_zj INT DEFAULT NULL COMMENT '最高租金',
    min_zj INT DEFAULT NULL COMMENT '最低租金',
    PRIMARY KEY (xzq)
) COMMENT='各行政区平均租金';

DROP TABLE IF EXISTS ads_fy_heatmap;
CREATE TABLE ads_fy_heatmap (
    sq VARCHAR(64) NOT NULL,
    xzq VARCHAR(64) DEFAULT NULL COMMENT '行政区',
    pj_zj DECIMAL(7,2) DEFAULT NULL COMMENT '平均租金',
    center_jd DECIMAL(9,6) DEFAULT NULL COMMENT '商圈中心经度',
    center_wd DECIMAL(9,6) DEFAULT NULL COMMENT '商圈中心纬度',
    fysl BIGINT DEFAULT 0 COMMENT '房源数量',
    PRIMARY KEY (sq)
) COMMENT='租金热力图数据';

DROP TABLE IF EXISTS ads_sq_top10;
CREATE TABLE ads_sq_top10 (
    sq VARCHAR(64) NOT NULL,
    xzq VARCHAR(64) DEFAULT NULL,
    fysl BIGINT DEFAULT 0,
    pj_zj DECIMAL(7,2) DEFAULT NULL,
    PRIMARY KEY (sq)
) COMMENT='商圈房源数量TOP10';

DROP TABLE IF EXISTS ads_fy_type_ratio;
CREATE TABLE ads_fy_type_ratio (
    fy_type VARCHAR(64) NOT NULL,
    fysl BIGINT DEFAULT 0,
    type_ratio DECIMAL(10,4) DEFAULT NULL,
    pj_zj DECIMAL(7,2) DEFAULT NULL,
    pj_mj DECIMAL(5,1) DEFAULT NULL,
    PRIMARY KEY (fy_type)
) COMMENT='房源类型占比';

DROP TABLE IF EXISTS ads_price_area_scatter;
CREATE TABLE ads_price_area_scatter (
    fy_id VARCHAR(32) NOT NULL,
    xzq VARCHAR(64) DEFAULT NULL,
    unit_dj DECIMAL(6,2) DEFAULT NULL COMMENT '租金单价(元/平方米/月)',
    jzmj DECIMAL(5,1) DEFAULT NULL COMMENT '建筑面积',
    month_zj INT DEFAULT NULL COMMENT '月租金',
    PRIMARY KEY (fy_id)
) COMMENT='租金单价与面积散点图';

DROP TABLE IF EXISTS ads_metro_rent_compare;
CREATE TABLE ads_metro_rent_compare (
    xzq VARCHAR(64) NOT NULL,
    is_dt VARCHAR(8) NOT NULL COMMENT '是否地铁房',
    fysl BIGINT DEFAULT 0,
    pj_zj DECIMAL(7,2) DEFAULT NULL,
    PRIMARY KEY (xzq, is_dt)
) COMMENT='地铁房与非地铁房租金对比';

DROP TABLE IF EXISTS ads_zx_avg_rent;
CREATE TABLE ads_zx_avg_rent (
    zx_qk VARCHAR(64) NOT NULL,
    fysl BIGINT DEFAULT 0,
    pj_zj DECIMAL(7,2) DEFAULT NULL,
    pj_dj DECIMAL(6,2) DEFAULT NULL COMMENT '平均单价',
    PRIMARY KEY (zx_qk)
) COMMENT='各装修情况平均租金';

DROP TABLE IF EXISTS ads_platform_distribution;
CREATE TABLE ads_platform_distribution (
    platform VARCHAR(64) NOT NULL,
    fysl BIGINT DEFAULT 0,
    pj_zj DECIMAL(7,2) DEFAULT NULL,
    PRIMARY KEY (platform)
) COMMENT='各平台房源分布';
