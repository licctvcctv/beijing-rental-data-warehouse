package com.beijing.wenyu.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LocalDwsLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> dwdTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("dws_region_summary", buildRegionSummary(dwdTables));
        tables.put("dws_movie_score_summary", buildMovieScoreSummary(requireTable(dwdTables, "dwd_movie_detail")));
        tables.put("dws_show_status_summary", buildShowStatusSummary(requireTable(dwdTables, "dwd_show_detail")));
        tables.put("dws_show_price_summary", buildShowPriceSummary(requireTable(dwdTables, "dwd_show_detail")));
        tables.put("dws_ktv_region_summary", buildKtvRegionSummary(requireTable(dwdTables, "dwd_ktv_detail")));
        tables.put("dws_sport_type_summary", buildSportTypeSummary(requireTable(dwdTables, "dwd_sport_detail")));
        tables.put("dws_scenic_visit_time_summary", buildScenicVisitSummary(requireTable(dwdTables, "dwd_scenic_detail")));
        return tables;
    }

    private LocalTable requireTable(Map<String, LocalTable> tables, String key) {
        LocalTable table = tables.get(key);
        if (table == null) {
            throw new IllegalStateException("Missing required DWD table: " + key);
        }
        return table;
    }

    private LocalTable buildRegionSummary(Map<String, LocalTable> dwdTables) {
        LocalTable table = new LocalTable("dws_region_summary", Arrays.asList("region", "category", "total_count"));
        appendRegionRows(table, countByField(requireTable(dwdTables, "dwd_scenic_detail"), "region_std"), "scenic");
        appendRegionRows(table, countByField(requireTable(dwdTables, "dwd_show_detail"), "region_std"), "show");
        appendRegionRows(table, countByField(requireTable(dwdTables, "dwd_ktv_detail"), "region_std"), "ktv");
        LinkedHashMap<String, String> movie = new LinkedHashMap<String, String>();
        movie.put("region", "北京市");
        movie.put("category", "movie");
        movie.put("total_count", String.valueOf(requireTable(dwdTables, "dwd_movie_detail").getRows().size()));
        table.addRow(movie);
        appendRegionRows(table, countByField(requireTable(dwdTables, "dwd_sport_detail"), "region_std"), "sport");
        return table;
    }

    private void appendRegionRows(LocalTable table, Map<String, Integer> counts, String category) {
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("region", entry.getKey());
            row.put("category", category);
            row.put("total_count", String.valueOf(entry.getValue()));
            table.addRow(row);
        }
    }

    private LocalTable buildMovieScoreSummary(LocalTable source) {
        Map<String, List<Double>> groups = new TreeMap<String, List<Double>>();
        for (Map<String, String> row : source.getRows()) {
            String level = row.get("score_level");
            if (!groups.containsKey(level)) {
                groups.put(level, new ArrayList<Double>());
            }
            groups.get(level).add(LocalValueUtils.parseDouble(row.get("score_num")));
        }
        List<Map.Entry<String, List<Double>>> entries = new ArrayList<Map.Entry<String, List<Double>>>(groups.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, List<Double>>>() {
            @Override
            public int compare(Map.Entry<String, List<Double>> left, Map.Entry<String, List<Double>> right) {
                int countCompare = Integer.valueOf(right.getValue().size()).compareTo(left.getValue().size());
                return countCompare != 0 ? countCompare : left.getKey().compareTo(right.getKey());
            }
        });
        LocalTable table = new LocalTable("dws_movie_score_summary", Arrays.asList("score_level", "movie_count", "avg_score"));
        for (Map.Entry<String, List<Double>> entry : entries) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("score_level", entry.getKey());
            row.put("movie_count", String.valueOf(entry.getValue().size()));
            row.put("avg_score", LocalValueUtils.formatDecimal(average(entry.getValue()), 2));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildShowStatusSummary(LocalTable source) {
        Map<String, List<Double>> groups = new TreeMap<String, List<Double>>();
        for (Map<String, String> row : source.getRows()) {
            String status = row.get("status_std");
            if (!groups.containsKey(status)) {
                groups.put(status, new ArrayList<Double>());
            }
            groups.get(status).add(LocalValueUtils.parseDouble(row.get("attention_num")));
        }
        List<Map.Entry<String, List<Double>>> entries = new ArrayList<Map.Entry<String, List<Double>>>(groups.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, List<Double>>>() {
            @Override
            public int compare(Map.Entry<String, List<Double>> left, Map.Entry<String, List<Double>> right) {
                int countCompare = Integer.valueOf(right.getValue().size()).compareTo(left.getValue().size());
                return countCompare != 0 ? countCompare : left.getKey().compareTo(right.getKey());
            }
        });
        LocalTable table = new LocalTable("dws_show_status_summary", Arrays.asList("status_std", "show_count", "avg_attention"));
        for (Map.Entry<String, List<Double>> entry : entries) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("status_std", entry.getKey());
            row.put("show_count", String.valueOf(entry.getValue().size()));
            row.put("avg_attention", LocalValueUtils.formatDecimal(average(entry.getValue()), 2));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildShowPriceSummary(LocalTable source) {
        LocalTable table = new LocalTable(
                "dws_show_price_summary",
                Arrays.asList("name", "venue", "region_std", "price_min", "price_max", "status_std", "attention_num")
        );
        for (Map<String, String> row : source.getRows()) {
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildKtvRegionSummary(LocalTable source) {
        Map<String, List<Map<String, String>>> groups = new TreeMap<String, List<Map<String, String>>>();
        for (Map<String, String> row : source.getRows()) {
            String region = row.get("region_std");
            if (!groups.containsKey(region)) {
                groups.put(region, new ArrayList<Map<String, String>>());
            }
            groups.get(region).add(row);
        }
        List<Map.Entry<String, List<Map<String, String>>>> entries = new ArrayList<Map.Entry<String, List<Map<String, String>>>>(groups.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, List<Map<String, String>>>>() {
            @Override
            public int compare(Map.Entry<String, List<Map<String, String>>> left, Map.Entry<String, List<Map<String, String>>> right) {
                int countCompare = Integer.valueOf(right.getValue().size()).compareTo(left.getValue().size());
                return countCompare != 0 ? countCompare : left.getKey().compareTo(right.getKey());
            }
        });
        LocalTable table = new LocalTable("dws_ktv_region_summary", Arrays.asList("region_std", "ktv_count", "avg_cost", "avg_score"));
        for (Map.Entry<String, List<Map<String, String>>> entry : entries) {
            List<Double> costs = new ArrayList<Double>();
            List<Double> scores = new ArrayList<Double>();
            for (Map<String, String> row : entry.getValue()) {
                costs.add(LocalValueUtils.parseDouble(row.get("avg_cost")));
                scores.add(LocalValueUtils.parseDouble(row.get("overall_score")));
            }
            LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
            result.put("region_std", entry.getKey());
            result.put("ktv_count", String.valueOf(entry.getValue().size()));
            result.put("avg_cost", LocalValueUtils.formatDecimal(average(costs), 2));
            result.put("avg_score", LocalValueUtils.formatDecimal(average(scores), 2));
            table.addRow(result);
        }
        return table;
    }

    private LocalTable buildSportTypeSummary(LocalTable source) {
        Map<String, List<Double>> groups = new TreeMap<String, List<Double>>();
        for (Map<String, String> row : source.getRows()) {
            String type = row.get("venue_type_std");
            if (!groups.containsKey(type)) {
                groups.put(type, new ArrayList<Double>());
            }
            groups.get(type).add(LocalValueUtils.parseDouble(row.get("score_num")));
        }
        List<Map.Entry<String, List<Double>>> entries = new ArrayList<Map.Entry<String, List<Double>>>(groups.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, List<Double>>>() {
            @Override
            public int compare(Map.Entry<String, List<Double>> left, Map.Entry<String, List<Double>> right) {
                int countCompare = Integer.valueOf(right.getValue().size()).compareTo(left.getValue().size());
                return countCompare != 0 ? countCompare : left.getKey().compareTo(right.getKey());
            }
        });
        LocalTable table = new LocalTable("dws_sport_type_summary", Arrays.asList("venue_type_std", "venue_count", "avg_score"));
        for (Map.Entry<String, List<Double>> entry : entries) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("venue_type_std", entry.getKey());
            row.put("venue_count", String.valueOf(entry.getValue().size()));
            row.put("avg_score", LocalValueUtils.formatDecimal(average(entry.getValue()), 2));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildScenicVisitSummary(LocalTable source) {
        Map<String, int[]> groups = new TreeMap<String, int[]>();
        for (Map<String, String> row : source.getRows()) {
            String bestVisitTime = row.get("best_visit_time");
            if (!groups.containsKey(bestVisitTime)) {
                groups.put(bestVisitTime, new int[]{0, 0});
            }
            int[] values = groups.get(bestVisitTime);
            values[0] = values[0] + 1;
            if (LocalValueUtils.parseDouble(row.get("price_min")) == 0D) {
                values[1] = values[1] + 1;
            }
        }
        LocalTable table = new LocalTable("dws_scenic_visit_time_summary", Arrays.asList("best_visit_time", "scenic_count", "free_count"));
        for (Map.Entry<String, int[]> entry : groups.entrySet()) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("best_visit_time", entry.getKey());
            row.put("scenic_count", String.valueOf(entry.getValue()[0]));
            row.put("free_count", String.valueOf(entry.getValue()[1]));
            table.addRow(row);
        }
        return table;
    }

    private Map<String, Integer> countByField(LocalTable table, String fieldName) {
        Map<String, Integer> counts = new TreeMap<String, Integer>();
        for (Map<String, String> row : table.getRows()) {
            String key = row.get(fieldName);
            Integer current = counts.get(key);
            counts.put(key, current == null ? 1 : current + 1);
        }
        return counts;
    }

    private double average(List<Double> values) {
        if (values.isEmpty()) {
            return 0D;
        }
        double total = 0D;
        for (Double value : values) {
            total += value;
        }
        return total / values.size();
    }
}
