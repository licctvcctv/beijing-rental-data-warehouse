package com.beijing.wenyu.local;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LocalSqlExporter {

    private static final List<String> ADS_TABLES = Arrays.asList(
            "ads_region_entertainment_count",
            "ads_movie_score_distribution",
            "ads_show_price_top10",
            "ads_show_status_ratio",
            "ads_ktv_region_hotspot",
            "ads_ktv_cost_performance_top5",
            "ads_sport_type_ratio_top5",
            "ads_scenic_free_ratio"
    );

    public void writeAdsSeedSql(File sqlFile, Map<String, LocalTable> adsTables) throws IOException {
        File parent = sqlFile.getParentFile();
        if (parent != null && !parent.exists()) {
            Files.createDirectories(parent.toPath());
        }
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(Files.newOutputStream(sqlFile.toPath()), StandardCharsets.UTF_8)
        );
        try {
            for (String tableName : ADS_TABLES) {
                LocalTable table = adsTables.get(tableName);
                writer.write("DELETE FROM " + tableName + ";");
                writer.newLine();
                if (table == null || table.getRows().isEmpty()) {
                    continue;
                }
                writer.write("INSERT INTO " + tableName + " (" + join(table.getFields()) + ") VALUES");
                writer.newLine();
                for (int i = 0; i < table.getRows().size(); i++) {
                    writer.write("  (" + sqlValues(table.getRows().get(i), table) + ")");
                    writer.write(i == table.getRows().size() - 1 ? ";" : ",");
                    writer.newLine();
                }
            }
        } finally {
            writer.close();
        }
    }

    private String sqlValues(Map<String, String> row, LocalTable table) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < table.getFields().size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(LocalValueUtils.sqlLiteral(row.get(table.getFields().get(i))));
        }
        return builder.toString();
    }

    private String join(List<String> values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(values.get(i));
        }
        return builder.toString();
    }
}
