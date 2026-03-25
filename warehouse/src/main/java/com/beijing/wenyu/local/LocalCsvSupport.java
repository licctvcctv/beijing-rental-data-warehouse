package com.beijing.wenyu.local;

import com.beijing.wenyu.common.WarehouseConstants;
import com.beijing.wenyu.etl.config.CategorySchema;
import com.beijing.wenyu.etl.util.CsvLineParser;
import com.beijing.wenyu.etl.util.TextNormalizeUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class LocalCsvSupport {

    private LocalCsvSupport() {
    }

    public static List<Map<String, String>> readCategory(String category, File csvFile) throws IOException {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        List<String> fields = CategorySchema.sourceFields(category);
        if (!csvFile.exists()) {
            return rows;
        }
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8)
        );
        try {
            String line;
            boolean headerSkipped = false;
            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }
                if (line.trim().isEmpty()) {
                    continue;
                }
                List<String> parsed = CsvLineParser.parse(stripUtf8Bom(line));
                if (parsed.size() < fields.size()) {
                    continue;
                }
                LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
                for (int i = 0; i < fields.size(); i++) {
                    row.put(fields.get(i), TextNormalizeUtils.normalize(parsed.get(i)));
                }
                rows.add(row);
            }
        } finally {
            reader.close();
        }
        return rows;
    }

    public static void writeTable(File outputFile, LocalTable table, String delimiter) throws IOException {
        ensureParent(outputFile);
        Writer writer = new BufferedWriter(
                new OutputStreamWriter(Files.newOutputStream(outputFile.toPath()), StandardCharsets.UTF_8)
        );
        try {
            writer.write(join(table.getFields(), delimiter));
            writer.write(System.lineSeparator());
            for (Map<String, String> row : table.getRows()) {
                List<String> values = new ArrayList<String>();
                for (String field : table.getFields()) {
                    values.add(escape(row.get(field), delimiter));
                }
                writer.write(join(values, delimiter));
                writer.write(System.lineSeparator());
            }
        } finally {
            writer.close();
        }
    }

    public static void writeSuccessFlag(File successFile) throws IOException {
        ensureParent(successFile);
        if (!successFile.exists()) {
            Files.createFile(successFile.toPath());
        }
    }

    public static void copyFile(File source, File target) throws IOException {
        ensureParent(target);
        Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private static String stripUtf8Bom(String line) {
        if (line != null && !line.isEmpty() && line.charAt(0) == '\uFEFF') {
            return line.substring(1);
        }
        return line;
    }

    private static void ensureParent(File file) throws IOException {
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            Files.createDirectories(parent.toPath());
        }
    }

    private static String join(List<String> values, String delimiter) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(delimiter);
            }
            builder.append(values.get(i));
        }
        return builder.toString();
    }

    private static String escape(String value, String delimiter) {
        String safe = LocalValueUtils.safe(value);
        if (WarehouseConstants.FIELD_DELIMITER.equals(delimiter)) {
            return safe;
        }
        boolean needQuotes = safe.contains(delimiter) || safe.contains("\"") || safe.contains("\n");
        if (!needQuotes) {
            return safe;
        }
        return "\"" + safe.replace("\"", "\"\"") + "\"";
    }
}
