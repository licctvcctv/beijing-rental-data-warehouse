package com.beijing.wenyu.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LocalTable {

    private final String name;
    private final List<String> fields;
    private final List<LinkedHashMap<String, String>> rows = new ArrayList<LinkedHashMap<String, String>>();

    public LocalTable(String name, List<String> fields) {
        this.name = name;
        this.fields = new ArrayList<String>(fields);
    }

    public String getName() {
        return name;
    }

    public List<String> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public List<LinkedHashMap<String, String>> getRows() {
        return Collections.unmodifiableList(rows);
    }

    public void addRow(Map<String, String> values) {
        LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
        for (String field : fields) {
            row.put(field, values.get(field));
        }
        rows.add(row);
    }
}
