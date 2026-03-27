package com.beijing.wenyu.local;

import com.beijing.wenyu.common.WarehouseConstants;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class LocalDwdLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> odsTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("dwd_fy_mx", buildRentalDetail(requireTable(odsTables, "ods_fy_jbxx")));
        return tables;
    }

    private LocalTable requireTable(Map<String, LocalTable> tables, String key) {
        LocalTable table = tables.get(key);
        if (table == null) {
            throw new IllegalStateException("Missing required ODS table: " + key);
        }
        return table;
    }

    private LocalTable buildRentalDetail(LocalTable source) {
        LocalTable table = new LocalTable(
                "dwd_fy_mx",
                Arrays.asList(
                        "fy_id", "fy_type", "platform", "xzq", "sq", "jd", "wd",
                        "month_zj", "jzmj", "unit_dj", "is_dt", "zx_qk"
                )
        );
        for (Map<String, String> row : source.getRows()) {
            String xzq = LocalValueUtils.isNull(row.get("xzq")) ? "未知" : row.get("xzq");
            String sq = LocalValueUtils.isNull(row.get("sq")) ? "未知" : row.get("sq");
            int monthZj = (int) LocalValueUtils.parseLong(row.get("month_zj"));
            double jzmj = LocalValueUtils.parseDouble(row.get("jzmj"));
            double unitDj = jzmj > 0 ? monthZj * 1.0 / jzmj : 0;
            String isDt = LocalValueUtils.isNull(row.get("is_dt")) ? "否" : row.get("is_dt");
            String zxQk = LocalValueUtils.isNull(row.get("zx_qk")) ? "未知" : row.get("zx_qk");

            LinkedHashMap<String, String> dwd = new LinkedHashMap<String, String>();
            dwd.put("fy_id", row.get("fy_id"));
            dwd.put("fy_type", row.get("fy_type"));
            dwd.put("platform", row.get("platform"));
            dwd.put("xzq", xzq);
            dwd.put("sq", sq);
            dwd.put("jd", row.get("jd"));
            dwd.put("wd", row.get("wd"));
            dwd.put("month_zj", String.valueOf(monthZj));
            dwd.put("jzmj", LocalValueUtils.formatDecimal(jzmj, 1));
            dwd.put("unit_dj", LocalValueUtils.formatDecimal(unitDj, 2));
            dwd.put("is_dt", isDt);
            dwd.put("zx_qk", zxQk);
            table.addRow(dwd);
        }
        return table;
    }
}
