package com.beijing.wenyu.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LocalAdsLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> dwdTables, Map<String, LocalTable> dwsTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("ads_xzq_avg_rent", cloneAs("ads_xzq_avg_rent", requireTable(dwsTables, "dws_fy_xzq_zj")));
        tables.put("ads_fy_heatmap", cloneAs("ads_fy_heatmap", requireTable(dwsTables, "dws_fy_sq_summary")));
        tables.put("ads_sq_top10", buildSqTop10(requireTable(dwsTables, "dws_fy_sq_summary")));
        tables.put("ads_fy_type_ratio", buildTypeRatio(requireTable(dwsTables, "dws_fy_type_summary")));
        tables.put("ads_price_area_scatter", buildScatter(requireTable(dwdTables, "dwd_fy_mx")));
        tables.put("ads_metro_rent_compare", cloneAs("ads_metro_rent_compare", requireTable(dwsTables, "dws_fy_metro_compare")));
        tables.put("ads_zx_avg_rent", cloneAs("ads_zx_avg_rent", requireTable(dwsTables, "dws_fy_zx_summary")));
        tables.put("ads_platform_distribution", cloneAs("ads_platform_distribution", requireTable(dwsTables, "dws_fy_platform_summary")));
        return tables;
    }

    private LocalTable requireTable(Map<String, LocalTable> tables, String key) {
        LocalTable table = tables.get(key);
        if (table == null) {
            throw new IllegalStateException("Missing required table: " + key);
        }
        return table;
    }

    private LocalTable buildSqTop10(LocalTable source) {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>(source.getRows());
        Collections.sort(rows, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> left, Map<String, String> right) {
                return Long.valueOf(LocalValueUtils.parseLong(right.get("fysl")))
                        .compareTo(LocalValueUtils.parseLong(left.get("fysl")));
            }
        });
        LocalTable table = new LocalTable("ads_sq_top10", Arrays.asList("sq", "xzq", "fysl", "pj_zj"));
        int limit = Math.min(10, rows.size());
        for (int i = 0; i < limit; i++) {
            Map<String, String> src = rows.get(i);
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("sq", src.get("sq"));
            row.put("xzq", src.get("xzq"));
            row.put("fysl", src.get("fysl"));
            row.put("pj_zj", src.get("pj_zj"));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildTypeRatio(LocalTable source) {
        long total = 0;
        for (Map<String, String> row : source.getRows()) {
            total += LocalValueUtils.parseLong(row.get("fysl"));
        }
        if (total <= 0) { total = 1; }
        LocalTable table = new LocalTable("ads_fy_type_ratio", Arrays.asList("fy_type", "fysl", "type_ratio", "pj_zj", "pj_mj"));
        for (Map<String, String> src : source.getRows()) {
            long count = LocalValueUtils.parseLong(src.get("fysl"));
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            row.put("fy_type", src.get("fy_type"));
            row.put("fysl", src.get("fysl"));
            row.put("type_ratio", LocalValueUtils.formatDecimal(count * 1.0 / total, 4));
            row.put("pj_zj", src.get("pj_zj"));
            row.put("pj_mj", src.get("pj_mj"));
            table.addRow(row);
        }
        return table;
    }

    private LocalTable buildScatter(LocalTable source) {
        LocalTable table = new LocalTable("ads_price_area_scatter", Arrays.asList("fy_id", "xzq", "unit_dj", "jzmj", "month_zj"));
        for (Map<String, String> src : source.getRows()) {
            double jzmj = LocalValueUtils.parseDouble(src.get("jzmj"));
            double unitDj = LocalValueUtils.parseDouble(src.get("unit_dj"));
            if (jzmj > 0 && unitDj > 0) {
                LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
                row.put("fy_id", src.get("fy_id"));
                row.put("xzq", src.get("xzq"));
                row.put("unit_dj", src.get("unit_dj"));
                row.put("jzmj", src.get("jzmj"));
                row.put("month_zj", src.get("month_zj"));
                table.addRow(row);
            }
        }
        return table;
    }

    private LocalTable cloneAs(String tableName, LocalTable source) {
        LocalTable target = new LocalTable(tableName, source.getFields());
        for (Map<String, String> row : source.getRows()) {
            target.addRow(row);
        }
        return target;
    }
}
