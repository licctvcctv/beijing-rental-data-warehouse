USE rental_dwd;

DROP TABLE IF EXISTS dwd_fy_mx;
CREATE TABLE dwd_fy_mx STORED AS PARQUET AS
SELECT
    fy_id,
    fy_type,
    platform,
    CASE
        WHEN xzq IS NULL OR TRIM(xzq) = '' OR xzq = '\\N' THEN '未知'
        ELSE TRIM(xzq)
    END AS xzq,
    CASE
        WHEN sq IS NULL OR TRIM(sq) = '' OR sq = '\\N' THEN '未知'
        ELSE TRIM(sq)
    END AS sq,
    jd,
    wd,
    COALESCE(month_zj, 0) AS month_zj,
    COALESCE(jzmj, CAST(0 AS DECIMAL(5,1))) AS jzmj,
    CASE
        WHEN jzmj > 0 THEN ROUND(month_zj / jzmj, 2)
        ELSE CAST(0 AS DECIMAL(6,2))
    END AS unit_dj,
    CASE
        WHEN is_dt IS NULL OR TRIM(is_dt) = '' OR is_dt = '\\N' THEN '否'
        ELSE TRIM(is_dt)
    END AS is_dt,
    CASE
        WHEN zx_qk IS NULL OR TRIM(zx_qk) = '' OR zx_qk = '\\N' THEN '未知'
        ELSE TRIM(zx_qk)
    END AS zx_qk
FROM rental_ods.ods_fy_jbxx
WHERE fy_id IS NOT NULL AND month_zj > 0;
