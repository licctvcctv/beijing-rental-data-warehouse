package com.beijing.wenyu.etl.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class CleanRecord {

    private final String category;
    private final LinkedHashMap<String, String> values = new LinkedHashMap<String, String>();

    public CleanRecord(String category) {
        this.category = category;
    }

    public String getCategory() {
        return category;
    }

    public void put(String key, String value) {
        values.put(key, value);
    }

    public String get(String key) {
        return values.get(key);
    }

    public Map<String, String> getValues() {
        return values;
    }
}
