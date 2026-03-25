package com.beijing.wenyu.metabase;

import java.util.Arrays;
import java.util.List;

public final class MetabaseCardRegistry {

    private MetabaseCardRegistry() {
    }

    public static List<CardSpec> cards() {
        return Arrays.asList(
                new CardSpec("娱乐资源总数", "来自 ads_region_entertainment_count", "progress",
                        "SELECT SUM(entertainment_count) AS value, 300 AS goal FROM ads_region_entertainment_count WHERE region <> '\\\\N';",
                        0, 0, 6, 3),
                new CardSpec("覆盖区域数", "来自 ads_region_entertainment_count", "progress",
                        "SELECT COUNT(*) AS value, 16 AS goal FROM ads_region_entertainment_count WHERE region NOT IN ('北京市', '\\\\N');",
                        0, 6, 6, 3),
                new CardSpec("在售演出数", "来自 ads_show_status_ratio", "progress",
                        "SELECT COALESCE(MAX(CASE WHEN status_std = '售票中' THEN show_count END), 0) AS value, 60 AS goal FROM ads_show_status_ratio;",
                        0, 12, 6, 3),
                new CardSpec("免费景点占比(%)", "来自 ads_scenic_free_ratio", "progress",
                        "SELECT ROUND(COALESCE(MAX(CASE WHEN scenic_type = '免费景点' THEN scenic_ratio END), 0) * 100, 2) AS value, 100 AS goal FROM ads_scenic_free_ratio;",
                        0, 18, 6, 3),
                new CardSpec("各区娱乐资源总量", "来自 ads_region_entertainment_count", "bar",
                        "SELECT region, entertainment_count FROM ads_region_entertainment_count WHERE region <> '\\\\N' ORDER BY entertainment_count DESC;",
                        3, 0, 12, 5),
                new CardSpec("娱乐类别分布", "来自 ADS 汇总表", "bar",
                        "SELECT category, total_count FROM (SELECT '景点' AS category, COALESCE(SUM(scenic_count), 0) AS total_count FROM ads_scenic_free_ratio UNION ALL SELECT '电影' AS category, COALESCE(SUM(movie_count), 0) AS total_count FROM ads_movie_score_distribution UNION ALL SELECT '演出' AS category, COALESCE(SUM(show_count), 0) AS total_count FROM ads_show_status_ratio UNION ALL SELECT 'KTV' AS category, COALESCE(SUM(ktv_count), 0) AS total_count FROM ads_ktv_region_hotspot UNION ALL SELECT '体育' AS category, COALESCE(SUM(venue_count), 0) AS total_count FROM ads_sport_type_ratio_top5) category_summary ORDER BY total_count DESC;",
                        3, 12, 6, 5),
                new CardSpec("演出价格榜单", "来自 ads_show_price_top10", "table",
                        "SELECT name, venue, price_max, status_std FROM ads_show_price_top10 ORDER BY price_max DESC LIMIT 6;",
                        3, 18, 6, 5),
                new CardSpec("电影评分分布", "来自 ads_movie_score_distribution", "pie",
                        "SELECT score_level, movie_count FROM ads_movie_score_distribution ORDER BY movie_count DESC;",
                        8, 0, 6, 5),
                new CardSpec("演出售票状态占比", "来自 ads_show_status_ratio", "pie",
                        "SELECT status_std, status_ratio FROM ads_show_status_ratio ORDER BY status_ratio DESC;",
                        8, 6, 6, 5),
                new CardSpec("KTV 性价比 Top5", "来自 ads_ktv_cost_performance_top5", "bar",
                        "SELECT name, cost_performance FROM ads_ktv_cost_performance_top5 ORDER BY cost_performance DESC LIMIT 5;",
                        8, 12, 6, 5),
                new CardSpec("体育场馆类型占比", "来自 ads_sport_type_ratio_top5", "pie",
                        "SELECT venue_type, venue_ratio FROM ads_sport_type_ratio_top5 ORDER BY venue_ratio DESC;",
                        8, 18, 6, 5)
        );
    }
}
