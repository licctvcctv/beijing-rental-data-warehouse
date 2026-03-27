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
        LocalTable dwd = requireTable(dwdTables, "dwd_fy_mx");
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("dws_fy_xzq_zj", buildXzqSummary(dwd));
        tables.put("dws_fy_sq_summary", buildSqSummary(dwd));
        tables.put("dws_fy_type_summary", buildTypeSummary(dwd));
        tables.put("dws_fy_metro_compare", buildMetroCompare(dwd));
        tables.put("dws_fy_zx_summary", buildZxSummary(dwd));
        tables.put("dws_fy_platform_summary", buildPlatformSummary(dwd));
        return tables;
    }

    private LocalTable requireTable(Map<String, LocalTable> tables, String key) {
        LocalTable table = tables.get(key);
        if (table == null) {
            throw new IllegalStateException("Missing required DWD table: " + key);
        }
        return table;
    }

    private LocalTable buildXzqSummary(LocalTable source) {
        Map<String, List<Integer>> groups = new TreeMap<String, List<Integer>>();
        for (Map<String, String> row : source.getRows()) {
            String xzq = row.get("xzq");
            if (!groups.containsKey(xzq)) {
                groups.put(xzq, new ArrayList<Integer>());
            }
            groups.get(xzq).add((int) LocalValueUtils.parseLong(row.get("month_zj")));
        }
        LocalTable table = new LocalTable("dws_fy_xzq_zj", Arrays.asList("xzq", "pj_zj", "fysl", "max_zj", "min_zj"));
        for (Map.Entry<String, List<Integer>> entry : groups.entrySet()) {
            List<Integer> rents = entry.getValue();
            int max = Integer.MIN_VALUE, min = Integer.MAX_VALUE;
            long sum = 0;
            for (int r : rents) { sum += r; max = Math.max(max, r); min = Math.min(min, r); }
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("xzq", entry.getKey());
            row.put("pj_zj", LocalValueUtils.formatDecimal(sum * 1.0 / rents.size(), 2));
            row.put("fysl", String.valueOf(rents.size()));
            row.put("max_zj", String.valueOf(max));
            row.put("min_zj", String.valueOf(min));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildSqSummary(LocalTable source) {
        Map<String, List<Map<String, String>>> groups = new TreeMap<String, List<Map<String, String>>>();
        for (Map<String, String> row : source.getRows()) {
            String key = row.get("sq") + "|" + row.get("xzq");
            if (!groups.containsKey(key)) {
                groups.put(key, new ArrayList<Map<String, String>>());
            }
            groups.get(key).add(row);
        }
        LocalTable table = new LocalTable("dws_fy_sq_summary", Arrays.asList("sq", "xzq", "fysl", "pj_zj", "center_jd", "center_wd"));
        for (Map.Entry<String, List<Map<String, String>>> entry : groups.entrySet()) {
            List<Map<String, String>> rows = entry.getValue();
            double sumZj = 0, sumJd = 0, sumWd = 0;
            for (Map<String, String> r : rows) {
                sumZj += LocalValueUtils.parseLong(r.get("month_zj"));
                sumJd += LocalValueUtils.parseDouble(r.get("jd"));
                sumWd += LocalValueUtils.parseDouble(r.get("wd"));
            }
            String[] parts = entry.getKey().split("\\|", 2);
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("sq", parts[0]);
            row.put("xzq", parts[1]);
            row.put("fysl", String.valueOf(rows.size()));
            row.put("pj_zj", LocalValueUtils.formatDecimal(sumZj / rows.size(), 2));
            row.put("center_jd", LocalValueUtils.formatDecimal(sumJd / rows.size(), 6));
            row.put("center_wd", LocalValueUtils.formatDecimal(sumWd / rows.size(), 6));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildTypeSummary(LocalTable source) {
        Map<String, List<Map<String, String>>> groups = new TreeMap<String, List<Map<String, String>>>();
        for (Map<String, String> row : source.getRows()) {
            String key = row.get("fy_type");
            if (!groups.containsKey(key)) {
                groups.put(key, new ArrayList<Map<String, String>>());
            }
            groups.get(key).add(row);
        }
        LocalTable table = new LocalTable("dws_fy_type_summary", Arrays.asList("fy_type", "fysl", "pj_zj", "pj_mj"));
        for (Map.Entry<String, List<Map<String, String>>> entry : groups.entrySet()) {
            List<Map<String, String>> rows = entry.getValue();
            double sumZj = 0, sumMj = 0;
            for (Map<String, String> r : rows) {
                sumZj += LocalValueUtils.parseLong(r.get("month_zj"));
                sumMj += LocalValueUtils.parseDouble(r.get("jzmj"));
            }
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("fy_type", entry.getKey());
            row.put("fysl", String.valueOf(rows.size()));
            row.put("pj_zj", LocalValueUtils.formatDecimal(sumZj / rows.size(), 2));
            row.put("pj_mj", LocalValueUtils.formatDecimal(sumMj / rows.size(), 1));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildMetroCompare(LocalTable source) {
        Map<String, List<Integer>> groups = new TreeMap<String, List<Integer>>();
        for (Map<String, String> row : source.getRows()) {
            String key = row.get("xzq") + "|" + row.get("is_dt");
            if (!groups.containsKey(key)) {
                groups.put(key, new ArrayList<Integer>());
            }
            groups.get(key).add((int) LocalValueUtils.parseLong(row.get("month_zj")));
        }
        LocalTable table = new LocalTable("dws_fy_metro_compare", Arrays.asList("xzq", "is_dt", "fysl", "pj_zj"));
        for (Map.Entry<String, List<Integer>> entry : groups.entrySet()) {
            List<Integer> rents = entry.getValue();
            long sum = 0;
            for (int r : rents) { sum += r; }
            String[] parts = entry.getKey().split("\\|", 2);
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("xzq", parts[0]);
            row.put("is_dt", parts[1]);
            row.put("fysl", String.valueOf(rents.size()));
            row.put("pj_zj", LocalValueUtils.formatDecimal(sum * 1.0 / rents.size(), 2));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildZxSummary(LocalTable source) {
        Map<String, List<Map<String, String>>> groups = new TreeMap<String, List<Map<String, String>>>();
        for (Map<String, String> row : source.getRows()) {
            String key = row.get("zx_qk");
            if (!groups.containsKey(key)) {
                groups.put(key, new ArrayList<Map<String, String>>());
            }
            groups.get(key).add(row);
        }
        LocalTable table = new LocalTable("dws_fy_zx_summary", Arrays.asList("zx_qk", "fysl", "pj_zj", "pj_dj"));
        for (Map.Entry<String, List<Map<String, String>>> entry : groups.entrySet()) {
            List<Map<String, String>> rows = entry.getValue();
            double sumZj = 0, sumDj = 0;
            for (Map<String, String> r : rows) {
                sumZj += LocalValueUtils.parseLong(r.get("month_zj"));
                sumDj += LocalValueUtils.parseDouble(r.get("unit_dj"));
            }
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("zx_qk", entry.getKey());
            row.put("fysl", String.valueOf(rows.size()));
            row.put("pj_zj", LocalValueUtils.formatDecimal(sumZj / rows.size(), 2));
            row.put("pj_dj", LocalValueUtils.formatDecimal(sumDj / rows.size(), 2));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildPlatformSummary(LocalTable source) {
        Map<String, List<Integer>> groups = new TreeMap<String, List<Integer>>();
        for (Map<String, String> row : source.getRows()) {
            String key = row.get("platform");
            if (!groups.containsKey(key)) {
                groups.put(key, new ArrayList<Integer>());
            }
            groups.get(key).add((int) LocalValueUtils.parseLong(row.get("month_zj")));
        }
        LocalTable table = new LocalTable("dws_fy_platform_summary", Arrays.asList("platform", "fysl", "pj_zj"));
        for (Map.Entry<String, List<Integer>> entry : groups.entrySet()) {
            List<Integer> rents = entry.getValue();
            long sum = 0;
            for (int r : rents) { sum += r; }
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("platform", entry.getKey());
            row.put("fysl", String.valueOf(rents.size()));
            row.put("pj_zj", LocalValueUtils.formatDecimal(sum * 1.0 / rents.size(), 2));
            table.addRow(row);
        }
        return table;
    }
}
