USE rental_dws;

-- 各行政区租金汇总
DROP TABLE IF EXISTS dws_fy_xzq_zj;
CREATE TABLE dws_fy_xzq_zj STORED AS PARQUET AS
SELECT
    xzq,
    ROUND(AVG(month_zj), 2) AS pj_zj,
    COUNT(fy_id) AS fysl,
    MAX(month_zj) AS max_zj,
    MIN(month_zj) AS min_zj
FROM rental_dwd.dwd_fy_mx
GROUP BY xzq;

-- 各商圈房源统计
DROP TABLE IF EXISTS dws_fy_sq_summary;
CREATE TABLE dws_fy_sq_summary STORED AS PARQUET AS
SELECT
    sq,
    xzq,
    COUNT(fy_id) AS fysl,
    ROUND(AVG(month_zj), 2) AS pj_zj,
    ROUND(AVG(jd), 6) AS center_jd,
    ROUND(AVG(wd), 6) AS center_wd
FROM rental_dwd.dwd_fy_mx
GROUP BY sq, xzq;

-- 房源类型汇总
DROP TABLE IF EXISTS dws_fy_type_summary;
CREATE TABLE dws_fy_type_summary STORED AS PARQUET AS
SELECT
    fy_type,
    COUNT(fy_id) AS fysl,
    ROUND(AVG(month_zj), 2) AS pj_zj,
    ROUND(AVG(jzmj), 1) AS pj_mj
FROM rental_dwd.dwd_fy_mx
GROUP BY fy_type;

-- 地铁房 vs 非地铁房按区对比
DROP TABLE IF EXISTS dws_fy_metro_compare;
CREATE TABLE dws_fy_metro_compare STORED AS PARQUET AS
SELECT
    xzq,
    is_dt,
    COUNT(fy_id) AS fysl,
    ROUND(AVG(month_zj), 2) AS pj_zj
FROM rental_dwd.dwd_fy_mx
GROUP BY xzq, is_dt;

-- 各装修情况汇总
DROP TABLE IF EXISTS dws_fy_zx_summary;
CREATE TABLE dws_fy_zx_summary STORED AS PARQUET AS
SELECT
    zx_qk,
    COUNT(fy_id) AS fysl,
    ROUND(AVG(month_zj), 2) AS pj_zj,
    ROUND(AVG(unit_dj), 2) AS pj_dj
FROM rental_dwd.dwd_fy_mx
GROUP BY zx_qk;

-- 各平台房源汇总
DROP TABLE IF EXISTS dws_fy_platform_summary;
CREATE TABLE dws_fy_platform_summary STORED AS PARQUET AS
SELECT
    platform,
    COUNT(fy_id) AS fysl,
    ROUND(AVG(month_zj), 2) AS pj_zj
FROM rental_dwd.dwd_fy_mx
GROUP BY platform;
