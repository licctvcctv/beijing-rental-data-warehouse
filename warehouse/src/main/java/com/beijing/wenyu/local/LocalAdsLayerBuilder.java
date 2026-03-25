package com.beijing.wenyu.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LocalAdsLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> dwdTables, Map<String, LocalTable> dwsTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("ads_region_entertainment_count", buildRegionCount(dwsTables.get("dws_region_summary")));
        tables.put("ads_movie_score_distribution", cloneAs("ads_movie_score_distribution", dwsTables.get("dws_movie_score_summary")));
        tables.put("ads_show_price_top10", buildShowPriceTop10(dwsTables.get("dws_show_price_summary")));
        tables.put("ads_show_status_ratio", buildShowStatusRatio(dwsTables.get("dws_show_status_summary")));
        tables.put("ads_ktv_region_hotspot", buildKtvHotspot(dwsTables.get("dws_ktv_region_summary")));
        tables.put("ads_ktv_cost_performance_top5", buildKtvPerformance(dwdTables.get("dwd_ktv_detail")));
        tables.put("ads_sport_type_ratio_top5", buildSportRatio(dwsTables.get("dws_sport_type_summary")));
        tables.put("ads_scenic_free_ratio", buildScenicRatio(dwdTables.get("dwd_scenic_detail")));
        return tables;
    }

    private LocalTable buildRegionCount(LocalTable source) {
        Map<String, Long> totals = new TreeMap<String, Long>();
        for (Map<String, String> row : source.getRows()) {
            String region = row.get("region");
            long total = totals.containsKey(region) ? totals.get(region) : 0L;
            totals.put(region, total + LocalValueUtils.parseLong(row.get("total_count")));
        }
        List<Map.Entry<String, Long>> entries = new ArrayList<Map.Entry<String, Long>>(totals.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> left, Map.Entry<String, Long> right) {
                int totalCompare = right.getValue().compareTo(left.getValue());
                return totalCompare != 0 ? totalCompare : left.getKey().compareTo(right.getKey());
            }
        });
        LocalTable table = new LocalTable("ads_region_entertainment_count", Arrays.asList("region", "entertainment_count"));
        for (Map.Entry<String, Long> entry : entries) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("region", entry.getKey());
            row.put("entertainment_count", String.valueOf(entry.getValue()));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildShowPriceTop10(LocalTable source) {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>(source.getRows());
        Collections.sort(rows, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> left, Map<String, String> right) {
                int maxCompare = Double.valueOf(LocalValueUtils.parseDouble(right.get("price_max")))
                        .compareTo(LocalValueUtils.parseDouble(left.get("price_max")));
                if (maxCompare != 0) {
                    return maxCompare;
                }
                int attentionCompare = Double.valueOf(LocalValueUtils.parseDouble(right.get("attention_num")))
                        .compareTo(LocalValueUtils.parseDouble(left.get("attention_num")));
                if (attentionCompare != 0) {
                    return attentionCompare;
                }
                return left.get("name").compareTo(right.get("name"));
            }
        });
        LocalTable table = new LocalTable(
                "ads_show_price_top10",
                Arrays.asList("name", "venue", "region", "price_max", "price_min", "status_std", "attention_num")
        );
        int limit = Math.min(10, rows.size());
        for (int i = 0; i < limit; i++) {
            Map<String, String> sourceRow = rows.get(i);
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("name", sourceRow.get("name"));
            row.put("venue", sourceRow.get("venue"));
            row.put("region", sourceRow.get("region_std"));
            row.put("price_max", sourceRow.get("price_max"));
            row.put("price_min", sourceRow.get("price_min"));
            row.put("status_std", sourceRow.get("status_std"));
            row.put("attention_num", sourceRow.get("attention_num"));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildShowStatusRatio(LocalTable source) {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        for (Map<String, String> row : source.getRows()) {
            if (!"待定".equals(row.get("status_std"))) {
                rows.add(row);
            }
        }
        if (rows.isEmpty()) {
            rows.addAll(source.getRows());
        }
        long total = 0L;
        for (Map<String, String> row : rows) {
            total += LocalValueUtils.parseLong(row.get("show_count"));
        }
        if (total <= 0) {
            total = 1L;
        }
        LocalTable table = new LocalTable("ads_show_status_ratio", Arrays.asList("status_std", "show_count", "status_ratio"));
        for (Map<String, String> row : rows) {
            double ratio = LocalValueUtils.parseLong(row.get("show_count")) * 1D / total;
            LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
            result.put("status_std", row.get("status_std"));
            result.put("show_count", row.get("show_count"));
            result.put("status_ratio", LocalValueUtils.formatDecimal(ratio, 4));
            table.addRow(result);
        }
        return table;
    }

    private LocalTable buildKtvHotspot(LocalTable source) {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>(source.getRows());
        Collections.sort(rows, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> left, Map<String, String> right) {
                int countCompare = Long.valueOf(LocalValueUtils.parseLong(right.get("ktv_count")))
                        .compareTo(LocalValueUtils.parseLong(left.get("ktv_count")));
                if (countCompare != 0) {
                    return countCompare;
                }
                int scoreCompare = Double.valueOf(LocalValueUtils.parseDouble(right.get("avg_score")))
                        .compareTo(LocalValueUtils.parseDouble(left.get("avg_score")));
                return scoreCompare != 0 ? scoreCompare : left.get("region_std").compareTo(right.get("region_std"));
            }
        });
        LocalTable table = new LocalTable("ads_ktv_region_hotspot", Arrays.asList("region", "ktv_count", "avg_cost", "avg_score"));
        for (Map<String, String> sourceRow : rows) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("region", sourceRow.get("region_std"));
            row.put("ktv_count", sourceRow.get("ktv_count"));
            row.put("avg_cost", sourceRow.get("avg_cost"));
            row.put("avg_score", sourceRow.get("avg_score"));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildKtvPerformance(LocalTable source) {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        for (Map<String, String> row : source.getRows()) {
            if (LocalValueUtils.parseDouble(row.get("avg_cost")) > 0D) {
                rows.add(row);
            }
        }
        Collections.sort(rows, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> left, Map<String, String> right) {
                int perfCompare = Double.valueOf(LocalValueUtils.parseDouble(right.get("cost_performance")))
                        .compareTo(LocalValueUtils.parseDouble(left.get("cost_performance")));
                if (perfCompare != 0) {
                    return perfCompare;
                }
                int popCompare = Long.valueOf(LocalValueUtils.parseLong(right.get("popularity_num")))
                        .compareTo(LocalValueUtils.parseLong(left.get("popularity_num")));
                return popCompare != 0 ? popCompare : left.get("name").compareTo(right.get("name"));
            }
        });
        LocalTable table = new LocalTable(
                "ads_ktv_cost_performance_top5",
                Arrays.asList("name", "region", "avg_cost", "overall_score", "cost_performance", "popularity_num")
        );
        int limit = Math.min(5, rows.size());
        for (int i = 0; i < limit; i++) {
            Map<String, String> sourceRow = rows.get(i);
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("name", sourceRow.get("name"));
            row.put("region", sourceRow.get("region_std"));
            row.put("avg_cost", sourceRow.get("avg_cost"));
            row.put("overall_score", sourceRow.get("overall_score"));
            row.put("cost_performance", sourceRow.get("cost_performance"));
            row.put("popularity_num", sourceRow.get("popularity_num"));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildSportRatio(LocalTable source) {
        long total = 0L;
        for (Map<String, String> row : source.getRows()) {
            total += LocalValueUtils.parseLong(row.get("venue_count"));
        }
        if (total <= 0) {
            total = 1L;
        }
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>(source.getRows());
        Collections.sort(rows, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> left, Map<String, String> right) {
                int countCompare = Long.valueOf(LocalValueUtils.parseLong(right.get("venue_count")))
                        .compareTo(LocalValueUtils.parseLong(left.get("venue_count")));
                return countCompare != 0 ? countCompare : left.get("venue_type_std").compareTo(right.get("venue_type_std"));
            }
        });
        LocalTable table = new LocalTable(
                "ads_sport_type_ratio_top5",
                Arrays.asList("venue_type", "venue_count", "venue_ratio", "avg_score")
        );
        int limit = Math.min(5, rows.size());
        for (int i = 0; i < limit; i++) {
            Map<String, String> sourceRow = rows.get(i);
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("venue_type", sourceRow.get("venue_type_std"));
            row.put("venue_count", sourceRow.get("venue_count"));
            row.put("venue_ratio", LocalValueUtils.formatDecimal(LocalValueUtils.parseLong(sourceRow.get("venue_count")) * 1D / total, 4));
            row.put("avg_score", sourceRow.get("avg_score"));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildScenicRatio(LocalTable source) {
        long total = source.getRows().size();
        if (total <= 0) {
            total = 1L;
        }
        long freeCount = 0L;
        long paidCount = 0L;
        for (Map<String, String> row : source.getRows()) {
            if (LocalValueUtils.parseDouble(row.get("price_min")) == 0D) {
                freeCount++;
            } else if (LocalValueUtils.parseDouble(row.get("price_min")) > 0D) {
                paidCount++;
            }
        }
        LocalTable table = new LocalTable("ads_scenic_free_ratio", Arrays.asList("scenic_type", "scenic_count", "scenic_ratio"));
        table.addRow(ratioRow("免费景点", freeCount, total));
        table.addRow(ratioRow("收费景点", paidCount, total));
        return table;
    }

    private LinkedHashMap<String, String> ratioRow(String label, long count, long total) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        row.put("scenic_type", label);
        row.put("scenic_count", String.valueOf(count));
        row.put("scenic_ratio", LocalValueUtils.formatDecimal(count * 1D / total, 4));
        return row;
    }

    private LocalTable cloneAs(String tableName, LocalTable source) {
        LocalTable target = new LocalTable(tableName, source.getFields());
        for (Map<String, String> row : source.getRows()) {
            target.addRow(row);
        }
        return target;
    }
}
