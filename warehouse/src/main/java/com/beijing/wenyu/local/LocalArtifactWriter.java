package com.beijing.wenyu.local;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

public class LocalArtifactWriter {

    public void writeAll(
            File rawDir,
            File outputDir,
            Map<String, LocalTable> cleanTables,
            Map<String, LocalTable> odsTables,
            Map<String, LocalTable> dwdTables,
            Map<String, LocalTable> dwsTables,
            Map<String, LocalTable> adsTables
    ) throws IOException {
        copyRaw(rawDir, new File(outputDir, "raw"));
        writeClean(cleanTables, new File(outputDir, "clean"));
        writeLayer(odsTables, new File(outputDir, "ods"));
        writeLayer(dwdTables, new File(outputDir, "dwd"));
        writeLayer(dwsTables, new File(outputDir, "dws"));
        writeLayer(adsTables, new File(outputDir, "ads"));
        new LocalSqlExporter().writeAdsSeedSql(new File(outputDir, "mysql/ads_seed.sql"), adsTables);
        writeSummary(new File(outputDir, "summary.txt"), cleanTables, odsTables, dwdTables, dwsTables, adsTables);
    }

    private void copyRaw(File rawDir, File targetDir) throws IOException {
        copyIfExists(new File(rawDir, "rental_raw.csv"), new File(targetDir, "rental_raw.csv"));
    }

    private void copyIfExists(File source, File target) throws IOException {
        if (source.exists()) {
            LocalCsvSupport.copyFile(source, target);
        }
    }

    private void writeClean(Map<String, LocalTable> tables, File layerDir) throws IOException {
        for (Map.Entry<String, LocalTable> entry : tables.entrySet()) {
            File categoryDir = new File(layerDir, entry.getKey());
            LocalCsvSupport.writeTable(new File(categoryDir, "part-00000.tsv"), entry.getValue(), "\t");
            LocalCsvSupport.writeSuccessFlag(new File(categoryDir, "_SUCCESS"));
        }
    }

    private void writeLayer(Map<String, LocalTable> tables, File layerDir) throws IOException {
        for (Map.Entry<String, LocalTable> entry : tables.entrySet()) {
            LocalCsvSupport.writeTable(new File(layerDir, entry.getKey() + ".csv"), entry.getValue(), ",");
        }
    }

    private void writeSummary(
            File summaryFile,
            Map<String, LocalTable> cleanTables,
            Map<String, LocalTable> odsTables,
            Map<String, LocalTable> dwdTables,
            Map<String, LocalTable> dwsTables,
            Map<String, LocalTable> adsTables
    ) throws IOException {
        File parent = summaryFile.getParentFile();
        if (parent != null && !parent.exists()) {
            Files.createDirectories(parent.toPath());
        }
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(Files.newOutputStream(summaryFile.toPath()), StandardCharsets.UTF_8)
        );
        try {
            writeSection(writer, "clean", cleanTables);
            writeSection(writer, "ods", odsTables);
            writeSection(writer, "dwd", dwdTables);
            writeSection(writer, "dws", dwsTables);
            writeSection(writer, "ads", adsTables);
        } finally {
            writer.close();
        }
    }

    private void writeSection(BufferedWriter writer, String section, Map<String, LocalTable> tables) throws IOException {
        writer.write("[" + section + "]");
        writer.newLine();
        for (Map.Entry<String, Integer> entry : counts(tables).entrySet()) {
            writer.write(entry.getKey() + "=" + entry.getValue());
            writer.newLine();
        }
        writer.newLine();
    }

    private Map<String, Integer> counts(Map<String, LocalTable> tables) {
        LinkedHashMap<String, Integer> counts = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, LocalTable> entry : tables.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().getRows().size());
        }
        return counts;
    }
}
