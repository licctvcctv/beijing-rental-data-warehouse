package com.beijing.wenyu.local;

import java.util.LinkedHashMap;
import java.util.Map;

public class LocalOdsLayerBuilder {

    public Map<String, LocalTable> build(Map<String, LocalTable> cleanTables) {
        LinkedHashMap<String, LocalTable> tables = new LinkedHashMap<String, LocalTable>();
        tables.put("ods_scenic_info", cloneAs("ods_scenic_info", cleanTables.get("scenic")));
        tables.put("ods_show_info", cloneAs("ods_show_info", cleanTables.get("show")));
        tables.put("ods_ktv_info", cloneAs("ods_ktv_info", cleanTables.get("ktv")));
        tables.put("ods_movie_info", cloneAs("ods_movie_info", cleanTables.get("movie")));
        tables.put("ods_sport_info", cloneAs("ods_sport_info", cleanTables.get("sport")));
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
