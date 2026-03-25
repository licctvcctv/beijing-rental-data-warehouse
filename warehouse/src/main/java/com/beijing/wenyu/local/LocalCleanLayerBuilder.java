package com.beijing.wenyu.local;

import com.beijing.wenyu.etl.util.CleanRuleUtils;
import com.beijing.wenyu.etl.util.NumberParseUtils;
import com.beijing.wenyu.etl.util.PriceParseUtils;
import com.beijing.wenyu.etl.util.RegionNormalizeUtils;
import com.beijing.wenyu.etl.util.TextNormalizeUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalCleanLayerBuilder {

    public Map<String, LocalTable> build(File rawDir) throws IOException {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("scenic", buildScenic(readCategory(rawDir, "scenic_raw.csv")));
        tables.put("show", buildShow(readCategory(rawDir, "show_raw.csv")));
        tables.put("ktv", buildKtv(readCategory(rawDir, "ktv_raw.csv")));
        tables.put("movie", buildMovie(readCategory(rawDir, "movie_raw.csv")));
        tables.put("sport", buildSport(readCategory(rawDir, "sport_raw.csv")));
        return tables;
    }

    private List<Map<String, String>> readCategory(File rawDir, String fileName) throws IOException {
        return LocalCsvSupport.readCategory(categoryName(fileName), new File(rawDir, fileName));
    }

    private String categoryName(String fileName) {
        return fileName.substring(0, fileName.indexOf("_raw.csv"));
    }

    private LocalTable buildScenic(List<Map<String, String>> rawRows) {
        LocalTable table = new LocalTable(
                "scenic",
                Arrays.asList(
                        "name", "level", "region", "address", "price", "price_min", "price_max",
                        "open_time", "visit_duration", "best_visit_time", "source_url", "source_site", "crawl_time"
                )
        );
        Set<String> seen = new LinkedHashSet<String>();
        for (Map<String, String> raw : rawRows) {
            LinkedHashMap<String, String> row = baseScenicRow(raw);
            if (TextNormalizeUtils.isNullValue(row.get("name"))) {
                continue;
            }
            String rawPrice = raw.get("price");
            row.put("price", PriceParseUtils.normalizeScenicPriceText(rawPrice));
            row.put("price_min", PriceParseUtils.extractScenicMinPrice(rawPrice));
            row.put("price_max", PriceParseUtils.extractScenicMaxPrice(rawPrice));
            row.put("best_visit_time", CleanRuleUtils.normalizeScenicBestVisit(raw.get("best_visit_time")));
            addIfAbsent(table, row, seen);
        }
        return table;
    }

    private LocalTable buildShow(List<Map<String, String>> rawRows) {
        LocalTable table = new LocalTable(
                "show",
                Arrays.asList(
                        "name", "show_time", "venue", "region", "price_range", "price_min", "price_max",
                        "status", "attention", "source_url", "source_site", "crawl_time"
                )
        );
        Set<String> seen = new LinkedHashSet<String>();
        for (Map<String, String> raw : rawRows) {
            LinkedHashMap<String, String> row = baseShowRow(raw);
            if (TextNormalizeUtils.isNullValue(row.get("name"))) {
                continue;
            }
            String rawPrice = raw.get("price_range");
            row.put("price_range", PriceParseUtils.normalizePriceText(rawPrice));
            row.put("price_min", PriceParseUtils.extractMinPrice(rawPrice));
            row.put("price_max", PriceParseUtils.extractMaxPrice(rawPrice));
            row.put("status", CleanRuleUtils.normalizeShowStatus(raw.get("status")));
            row.put("attention", NumberParseUtils.extractDecimal(raw.get("attention")));
            addIfAbsent(table, row, seen);
        }
        return table;
    }

    private LocalTable buildKtv(List<Map<String, String>> rawRows) {
        LocalTable table = new LocalTable(
                "ktv",
                Arrays.asList(
                        "name", "region", "address", "avg_cost", "service_score", "env_score", "overall_score",
                        "popularity", "business_hours", "source_url", "source_site", "crawl_time"
                )
        );
        Set<String> seen = new LinkedHashSet<String>();
        for (Map<String, String> raw : rawRows) {
            LinkedHashMap<String, String> row = baseKtvRow(raw);
            if (TextNormalizeUtils.isNullValue(row.get("name"))) {
                continue;
            }
            row.put("avg_cost", NumberParseUtils.extractDecimal(raw.get("avg_cost")));
            row.put("service_score", NumberParseUtils.extractDecimal(raw.get("service_score")));
            row.put("env_score", NumberParseUtils.extractDecimal(raw.get("env_score")));
            row.put("overall_score", NumberParseUtils.extractDecimal(raw.get("overall_score")));
            row.put("popularity", NumberParseUtils.extractInteger(raw.get("popularity")));
            addIfAbsent(table, row, seen);
        }
        return table;
    }

    private LocalTable buildMovie(List<Map<String, String>> rawRows) {
        LocalTable table = new LocalTable(
                "movie",
                Arrays.asList(
                        "name", "score", "category", "country_region", "director", "actors",
                        "intro", "source_url", "source_site", "crawl_time"
                )
        );
        Set<String> seen = new LinkedHashSet<String>();
        for (Map<String, String> raw : rawRows) {
            LinkedHashMap<String, String> row = baseMovieRow(raw);
            if (TextNormalizeUtils.isNullValue(row.get("name"))) {
                continue;
            }
            row.put("score", NumberParseUtils.extractDecimal(raw.get("score")));
            addIfAbsent(table, row, seen);
        }
        return table;
    }

    private LocalTable buildSport(List<Map<String, String>> rawRows) {
        LocalTable table = new LocalTable(
                "sport",
                Arrays.asList(
                        "name", "venue_type", "region", "address", "score", "comment_count", "avg_cost",
                        "open_time", "source_url", "source_site", "crawl_time"
                )
        );
        Set<String> seen = new LinkedHashSet<String>();
        for (Map<String, String> raw : rawRows) {
            LinkedHashMap<String, String> row = baseSportRow(raw);
            if (TextNormalizeUtils.isNullValue(row.get("name"))) {
                continue;
            }
            String venueType = CleanRuleUtils.normalizeSportType(raw.get("venue_type"));
            if (TextNormalizeUtils.isNullValue(venueType)) {
                venueType = inferSportType(row.get("name"));
            }
            row.put("venue_type", venueType);
            row.put("score", NumberParseUtils.extractDecimal(raw.get("score")));
            row.put("comment_count", NumberParseUtils.extractInteger(raw.get("comment_count")));
            row.put("avg_cost", NumberParseUtils.extractDecimal(raw.get("avg_cost")));
            addIfAbsent(table, row, seen);
        }
        return table;
    }

    private LinkedHashMap<String, String> baseScenicRow(Map<String, String> raw) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        row.put("name", TextNormalizeUtils.normalize(raw.get("name")));
        row.put("level", TextNormalizeUtils.normalize(raw.get("level")));
        row.put("region", RegionNormalizeUtils.normalize(raw.get("region"), raw.get("address")));
        row.put("address", TextNormalizeUtils.normalize(raw.get("address")));
        row.put("open_time", TextNormalizeUtils.normalize(raw.get("open_time")));
        row.put("visit_duration", TextNormalizeUtils.normalize(raw.get("visit_duration")));
        row.put("best_visit_time", TextNormalizeUtils.normalize(raw.get("best_visit_time")));
        row.put("source_url", TextNormalizeUtils.normalize(raw.get("source_url")));
        row.put("source_site", TextNormalizeUtils.normalize(raw.get("source_site")));
        row.put("crawl_time", TextNormalizeUtils.normalize(raw.get("crawl_time")));
        return row;
    }

    private LinkedHashMap<String, String> baseShowRow(Map<String, String> raw) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        row.put("name", TextNormalizeUtils.normalize(raw.get("name")));
        row.put("show_time", TextNormalizeUtils.normalize(raw.get("show_time")));
        row.put("venue", TextNormalizeUtils.normalize(raw.get("venue")));
        row.put("region", RegionNormalizeUtils.normalize(raw.get("region"), raw.get("venue")));
        row.put("source_url", TextNormalizeUtils.normalize(raw.get("source_url")));
        row.put("source_site", TextNormalizeUtils.normalize(raw.get("source_site")));
        row.put("crawl_time", TextNormalizeUtils.normalize(raw.get("crawl_time")));
        return row;
    }

    private LinkedHashMap<String, String> baseKtvRow(Map<String, String> raw) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        row.put("name", TextNormalizeUtils.normalize(raw.get("name")));
        row.put("region", RegionNormalizeUtils.normalize(raw.get("region"), raw.get("address")));
        row.put("address", TextNormalizeUtils.normalize(raw.get("address")));
        row.put("business_hours", TextNormalizeUtils.normalize(raw.get("business_hours")));
        row.put("source_url", TextNormalizeUtils.normalize(raw.get("source_url")));
        row.put("source_site", TextNormalizeUtils.normalize(raw.get("source_site")));
        row.put("crawl_time", TextNormalizeUtils.normalize(raw.get("crawl_time")));
        return row;
    }

    private LinkedHashMap<String, String> baseMovieRow(Map<String, String> raw) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        row.put("name", TextNormalizeUtils.normalize(raw.get("name")));
        row.put("category", TextNormalizeUtils.normalize(raw.get("category")));
        row.put("country_region", TextNormalizeUtils.normalize(raw.get("country_region")));
        row.put("director", TextNormalizeUtils.normalize(raw.get("director")));
        row.put("actors", TextNormalizeUtils.normalize(raw.get("actors")));
        row.put("intro", TextNormalizeUtils.normalize(raw.get("intro")));
        row.put("source_url", TextNormalizeUtils.normalize(raw.get("source_url")));
        row.put("source_site", TextNormalizeUtils.normalize(raw.get("source_site")));
        row.put("crawl_time", TextNormalizeUtils.normalize(raw.get("crawl_time")));
        return row;
    }

    private LinkedHashMap<String, String> baseSportRow(Map<String, String> raw) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        row.put("name", TextNormalizeUtils.normalize(raw.get("name")));
        row.put("region", RegionNormalizeUtils.normalize(raw.get("region"), raw.get("address")));
        row.put("address", TextNormalizeUtils.normalize(raw.get("address")));
        row.put("open_time", TextNormalizeUtils.normalize(raw.get("open_time")));
        row.put("source_url", TextNormalizeUtils.normalize(raw.get("source_url")));
        row.put("source_site", TextNormalizeUtils.normalize(raw.get("source_site")));
        row.put("crawl_time", TextNormalizeUtils.normalize(raw.get("crawl_time")));
        return row;
    }

    private String inferSportType(String name) {
        if (name == null) {
            return "\\N";
        }
        if (name.contains("游泳")) {
            return "游泳馆";
        }
        if (name.contains("滑冰") || name.contains("滑雪")) {
            return "冰雪场馆";
        }
        if (name.contains("网球")) {
            return "网球场馆";
        }
        if (name.contains("高尔夫")) {
            return "高尔夫场馆";
        }
        if (name.contains("足球")) {
            return "足球场";
        }
        if (name.contains("篮球")) {
            return "篮球馆";
        }
        return "\\N";
    }

    private void addIfAbsent(LocalTable table, LinkedHashMap<String, String> row, Set<String> seen) {
        StringBuilder signature = new StringBuilder();
        for (String field : table.getFields()) {
            if (signature.length() > 0) {
                signature.append('\u0001');
            }
            signature.append(LocalValueUtils.safe(row.get(field)));
        }
        if (seen.add(signature.toString())) {
            table.addRow(row);
        }
    }
}
