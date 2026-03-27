package com.beijing.wenyu.metabase;

import java.util.Arrays;
import java.util.List;

public final class MetabaseCardRegistry {

    private MetabaseCardRegistry() {
    }

    public static List<CardSpec> cards() {
        return Arrays.asList(
                new CardSpec("房源总数", "来自 ads_xzq_avg_rent", "progress",
                        "SELECT SUM(fysl) AS value, 10000 AS goal FROM ads_xzq_avg_rent;",
                        0, 0, 6, 3),
                new CardSpec("覆盖行政区数", "来自 ads_xzq_avg_rent", "progress",
                        "SELECT COUNT(*) AS value, 16 AS goal FROM ads_xzq_avg_rent;",
                        0, 6, 6, 3),
                new CardSpec("全市平均月租金", "来自 ads_xzq_avg_rent", "progress",
                        "SELECT ROUND(SUM(pj_zj * fysl) / SUM(fysl), 0) AS value, 10000 AS goal FROM ads_xzq_avg_rent;",
                        0, 12, 6, 3),
                new CardSpec("地铁房占比(%)", "来自 ads_metro_rent_compare", "progress",
                        "SELECT ROUND(SUM(CASE WHEN is_dt = '是' THEN fysl ELSE 0 END) * 100.0 / SUM(fysl), 2) AS value, 100 AS goal FROM ads_metro_rent_compare;",
                        0, 18, 6, 3),
                new CardSpec("各行政区平均租金", "来自 ads_xzq_avg_rent", "bar",
                        "SELECT xzq, pj_zj FROM ads_xzq_avg_rent ORDER BY pj_zj DESC;",
                        3, 0, 12, 5),
                new CardSpec("商圈房源数量TOP10", "来自 ads_sq_top10", "bar",
                        "SELECT sq, fysl FROM ads_sq_top10 ORDER BY fysl DESC;",
                        3, 12, 6, 5),
                new CardSpec("房源类型占比", "来自 ads_fy_type_ratio", "pie",
                        "SELECT fy_type, fysl FROM ads_fy_type_ratio ORDER BY fysl DESC;",
                        3, 18, 6, 5),
                new CardSpec("地铁房vs非地铁房租金", "来自 ads_metro_rent_compare", "bar",
                        "SELECT xzq, MAX(CASE WHEN is_dt = '是' THEN pj_zj END) AS metro_rent, MAX(CASE WHEN is_dt = '否' THEN pj_zj END) AS non_metro_rent FROM ads_metro_rent_compare GROUP BY xzq ORDER BY metro_rent DESC;",
                        8, 0, 12, 5),
                new CardSpec("各装修情况平均租金", "来自 ads_zx_avg_rent", "bar",
                        "SELECT zx_qk, pj_zj FROM ads_zx_avg_rent ORDER BY pj_zj DESC;",
                        8, 12, 6, 5),
                new CardSpec("各平台房源分布", "来自 ads_platform_distribution", "pie",
                        "SELECT platform, fysl FROM ads_platform_distribution ORDER BY fysl DESC;",
                        8, 18, 6, 5)
        );
    }
}
