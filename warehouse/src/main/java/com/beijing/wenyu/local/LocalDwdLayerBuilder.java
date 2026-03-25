package com.beijing.wenyu.local;

import com.beijing.wenyu.common.WarehouseConstants;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class LocalDwdLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> odsTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("dwd_scenic_detail", buildScenic(odsTables.get("ods_scenic_info")));
        tables.put("dwd_show_detail", buildShow(odsTables.get("ods_show_info")));
        tables.put("dwd_ktv_detail", buildKtv(odsTables.get("ods_ktv_info")));
        tables.put("dwd_movie_detail", buildMovie(odsTables.get("ods_movie_info")));
        tables.put("dwd_sport_detail", buildSport(odsTables.get("ods_sport_info")));
        return tables;
    }

    private LocalTable buildScenic(LocalTable source) {
        LocalTable table = new LocalTable(
                "dwd_scenic_detail",
                Arrays.asList(
                        "name", "level", "region_std", "address", "price", "price_min", "price_max",
                        "price_level", "open_time", "visit_duration", "best_visit_time", "etl_time", "source_url", "source_site"
                )
        );
        for (Map<String, String> row : source.getRows()) {
            String priceMin = LocalValueUtils.decimalOrZero(row.get("price_min"));
            String priceMax = LocalValueUtils.decimalOrFallback(row.get("price_max"), priceMin);
            double min = LocalValueUtils.parseDouble(priceMin);
            LinkedHashMap<String, String> dwd = new LinkedHashMap<String, String>();
            dwd.put("name", row.get("name"));
            dwd.put("level", row.get("level"));
            dwd.put("region_std", LocalValueUtils.isNull(row.get("region")) ? "未知" : row.get("region"));
            dwd.put("address", row.get("address"));
            dwd.put("price", row.get("price"));
            dwd.put("price_min", priceMin);
            dwd.put("price_max", priceMax);
            dwd.put("price_level", scenicPriceLevel(min));
            dwd.put("open_time", row.get("open_time"));
            dwd.put("visit_duration", row.get("visit_duration"));
            dwd.put("best_visit_time", row.get("best_visit_time"));
            dwd.put("etl_time", row.get("crawl_time"));
            dwd.put("source_url", row.get("source_url"));
            dwd.put("source_site", row.get("source_site"));
            table.addRow(dwd);
        }
        return table;
    }

    private LocalTable buildShow(LocalTable source) {
        LocalTable table = new LocalTable(
                "dwd_show_detail",
                Arrays.asList(
                        "name", "show_time", "venue", "region_std", "price_range", "price_min", "price_max",
                        "status_std", "attention_num", "etl_time", "source_url", "source_site"
                )
        );
        for (Map<String, String> row : source.getRows()) {
            LinkedHashMap<String, String> dwd = new LinkedHashMap<String, String>();
            dwd.put("name", row.get("name"));
            dwd.put("show_time", row.get("show_time"));
            dwd.put("venue", row.get("venue"));
            dwd.put("region_std", normalizeShowRegion(row.get("region")));
            dwd.put("price_range", row.get("price_range"));
            dwd.put("price_min", LocalValueUtils.decimalOrZero(row.get("price_min")));
            dwd.put("price_max", LocalValueUtils.decimalOrFallback(row.get("price_max"), row.get("price_min")));
            dwd.put("status_std", normalizeShowStatus(row.get("status")));
            dwd.put("attention_num", LocalValueUtils.decimalOrZero(row.get("attention")));
            dwd.put("etl_time", row.get("crawl_time"));
            dwd.put("source_url", row.get("source_url"));
            dwd.put("source_site", row.get("source_site"));
            table.addRow(dwd);
        }
        return table;
    }

    private LocalTable buildKtv(LocalTable source) {
        LocalTable table = new LocalTable(
                "dwd_ktv_detail",
                Arrays.asList(
                        "name", "region_std", "address", "avg_cost", "service_score", "env_score", "overall_score",
                        "popularity_num", "business_hours", "cost_performance", "etl_time", "source_url", "source_site"
                )
        );
        for (Map<String, String> row : source.getRows()) {
            String avgCost = LocalValueUtils.decimalOrZero(row.get("avg_cost"));
            String overallScore = LocalValueUtils.decimalOrZero(row.get("overall_score"));
            double avg = LocalValueUtils.parseDouble(avgCost);
            double score = LocalValueUtils.parseDouble(overallScore);
            LinkedHashMap<String, String> dwd = new LinkedHashMap<String, String>();
            dwd.put("name", row.get("name"));
            dwd.put("region_std", LocalValueUtils.isNull(row.get("region")) ? "未知" : row.get("region"));
            dwd.put("address", row.get("address"));
            dwd.put("avg_cost", avgCost);
            dwd.put("service_score", LocalValueUtils.decimalOrZero(row.get("service_score")));
            dwd.put("env_score", LocalValueUtils.decimalOrZero(row.get("env_score")));
            dwd.put("overall_score", overallScore);
            dwd.put("popularity_num", LocalValueUtils.isNull(row.get("popularity")) ? "0" : row.get("popularity"));
            dwd.put("business_hours", row.get("business_hours"));
            dwd.put("cost_performance", avg <= 0 ? "0" : LocalValueUtils.formatDecimal(score / avg, 4));
            dwd.put("etl_time", row.get("crawl_time"));
            dwd.put("source_url", row.get("source_url"));
            dwd.put("source_site", row.get("source_site"));
            table.addRow(dwd);
        }
        return table;
    }

    private LocalTable buildMovie(LocalTable source) {
        LocalTable table = new LocalTable(
                "dwd_movie_detail",
                Arrays.asList(
                        "name", "score_num", "category", "country_region", "director", "actors",
                        "intro", "score_level", "etl_time", "source_url", "source_site"
                )
        );
        for (Map<String, String> row : source.getRows()) {
            String scoreNum = LocalValueUtils.decimalOrZero(row.get("score"));
            double score = LocalValueUtils.parseDouble(scoreNum);
            LinkedHashMap<String, String> dwd = new LinkedHashMap<String, String>();
            dwd.put("name", row.get("name"));
            dwd.put("score_num", scoreNum);
            dwd.put("category", row.get("category"));
            dwd.put("country_region", row.get("country_region"));
            dwd.put("director", row.get("director"));
            dwd.put("actors", row.get("actors"));
            dwd.put("intro", row.get("intro"));
            dwd.put("score_level", scoreLevel(score));
            dwd.put("etl_time", row.get("crawl_time"));
            dwd.put("source_url", row.get("source_url"));
            dwd.put("source_site", row.get("source_site"));
            table.addRow(dwd);
        }
        return table;
    }

    private LocalTable buildSport(LocalTable source) {
        LocalTable table = new LocalTable(
                "dwd_sport_detail",
                Arrays.asList(
                        "name", "venue_type_std", "region_std", "address", "score_num", "comment_count_num",
                        "avg_cost", "open_time", "etl_time", "source_url", "source_site"
                )
        );
        for (Map<String, String> row : source.getRows()) {
            LinkedHashMap<String, String> dwd = new LinkedHashMap<String, String>();
            dwd.put("name", row.get("name"));
            dwd.put("venue_type_std", LocalValueUtils.isNull(row.get("venue_type")) ? "其他场馆" : row.get("venue_type"));
            dwd.put("region_std", LocalValueUtils.isNull(row.get("region")) ? "未知" : row.get("region"));
            dwd.put("address", row.get("address"));
            dwd.put("score_num", LocalValueUtils.decimalOrZero(row.get("score")));
            dwd.put("comment_count_num", LocalValueUtils.isNull(row.get("comment_count")) ? "0" : row.get("comment_count"));
            dwd.put("avg_cost", LocalValueUtils.decimalOrZero(row.get("avg_cost")));
            dwd.put("open_time", row.get("open_time"));
            dwd.put("etl_time", row.get("crawl_time"));
            dwd.put("source_url", row.get("source_url"));
            dwd.put("source_site", row.get("source_site"));
            table.addRow(dwd);
        }
        return table;
    }

    private String scenicPriceLevel(double priceMin) {
        if (priceMin <= 0) {
            return "免费/低价";
        }
        if (priceMin <= 50) {
            return "50元内";
        }
        return "50元以上";
    }

    private String normalizeShowRegion(String region) {
        if (WarehouseConstants.NULL_VALUE.equals(region) || "北京市".equals(region)) {
            return "北京市";
        }
        return region;
    }

    private String normalizeShowStatus(String status) {
        if (WarehouseConstants.NULL_VALUE.equals(status)) {
            return "待定";
        }
        if ("售票中".equals(status) || "预售中".equals(status) || "已结束".equals(status)) {
            return status;
        }
        return status;
    }

    private String scoreLevel(double score) {
        if (score >= 9) {
            return "9分及以上";
        }
        if (score >= 8) {
            return "8-9分";
        }
        if (score >= 7) {
            return "7-8分";
        }
        if (score > 0) {
            return "7分以下";
        }
        return "暂无评分";
    }
}
