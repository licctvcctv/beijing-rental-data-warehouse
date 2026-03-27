package com.beijing.wenyu.local;

import com.beijing.wenyu.etl.util.NumberParseUtils;
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
        tables.put("rental", buildRental(readCategory(rawDir, "rental_raw.csv")));
        return tables;
    }

    private List<Map<String, String>> readCategory(File rawDir, String fileName) throws IOException {
        return LocalCsvSupport.readCategory(categoryName(fileName), new File(rawDir, fileName));
    }

    private String categoryName(String fileName) {
        return fileName.substring(0, fileName.indexOf("_raw.csv"));
    }

    private LocalTable buildRental(List<Map<String, String>> rawRows) {
        LocalTable table = new LocalTable(
                "rental",
                Arrays.asList(
                        "fy_id", "fy_title", "fy_type", "fy_status", "platform",
                        "xzq", "sq", "jd", "wd", "month_zj", "jzmj", "is_dt", "zx_qk"
                )
        );
        Set<String> seen = new LinkedHashSet<String>();
        for (Map<String, String> raw : rawRows) {
            LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
            String fyId = TextNormalizeUtils.normalize(raw.get("fy_id"));
            if (TextNormalizeUtils.isNullValue(fyId)) {
                continue;
            }
            String monthZj = NumberParseUtils.extractInteger(raw.get("month_zj"));
            if ("0".equals(monthZj) || "\\N".equals(monthZj)) {
                continue;
            }
            row.put("fy_id", fyId);
            row.put("fy_title", TextNormalizeUtils.normalize(raw.get("fy_title")));
            row.put("fy_type", TextNormalizeUtils.normalize(raw.get("fy_type")));
            row.put("fy_status", TextNormalizeUtils.normalize(raw.get("fy_status")));
            row.put("platform", TextNormalizeUtils.normalize(raw.get("platform")));
            row.put("xzq", TextNormalizeUtils.normalize(raw.get("xzq")));
            row.put("sq", TextNormalizeUtils.normalize(raw.get("sq")));
            row.put("jd", NumberParseUtils.extractDecimal(raw.get("jd")));
            row.put("wd", NumberParseUtils.extractDecimal(raw.get("wd")));
            row.put("month_zj", monthZj);
            row.put("jzmj", NumberParseUtils.extractDecimal(raw.get("jzmj")));
            row.put("is_dt", TextNormalizeUtils.normalize(raw.get("is_dt")));
            row.put("zx_qk", TextNormalizeUtils.normalize(raw.get("zx_qk")));
            addIfAbsent(table, row, seen);
        }
        return table;
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
