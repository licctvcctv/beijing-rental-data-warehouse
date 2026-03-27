package com.beijing.wenyu.local;

import java.util.LinkedHashMap;
import java.util.Map;

public class LocalOdsLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> cleanTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("ods_fy_jbxx", cloneAs("ods_fy_jbxx", cleanTables.get("rental")));
        return tables;
    }

    private LocalTable cloneAs(String tableName, LocalTable source) {
        LocalTable target = new LocalTable(tableName, source.getFields());
        for (Map<String, String> row : source.getRows()) {
            target.addRow(row);
        }
        return target;
    }
}
