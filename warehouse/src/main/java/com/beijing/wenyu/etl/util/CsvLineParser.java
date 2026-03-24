package com.beijing.wenyu.etl.util;

import java.util.ArrayList;
import java.util.List;

public final class CsvLineParser {

    private CsvLineParser() {
    }

    public static List<String> parse(String line) {
        List<String> fields = new ArrayList<String>();
        if (line == null) {
            return fields;
        }
        StringBuilder builder = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char current = line.charAt(i);
            if (current == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    builder.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (current == ',' && !inQuotes) {
                fields.add(builder.toString());
                builder.setLength(0);
            } else {
                builder.append(current);
            }
        }
        fields.add(builder.toString());
        return fields;
    }
}
