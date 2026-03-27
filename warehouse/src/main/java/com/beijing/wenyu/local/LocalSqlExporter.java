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
            "ads_xzq_avg_rent",
            "ads_fy_heatmap",
            "ads_sq_top10",
            "ads_fy_type_ratio",
            "ads_price_area_scatter",
            "ads_metro_rent_compare",
            "ads_zx_avg_rent",
            "ads_platform_distribution"
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
