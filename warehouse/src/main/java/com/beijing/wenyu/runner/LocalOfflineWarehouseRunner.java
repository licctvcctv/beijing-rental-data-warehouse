package com.beijing.wenyu.runner;

import com.beijing.wenyu.common.WarehouseConfig;
import com.beijing.wenyu.local.LocalAdsLayerBuilder;
import com.beijing.wenyu.local.LocalArtifactWriter;
import com.beijing.wenyu.local.LocalCleanLayerBuilder;
import com.beijing.wenyu.local.LocalDwdLayerBuilder;
import com.beijing.wenyu.local.LocalDwsLayerBuilder;
import com.beijing.wenyu.local.LocalOdsLayerBuilder;
import com.beijing.wenyu.local.LocalTable;

import java.io.File;
import java.util.Map;

public class LocalOfflineWarehouseRunner {

    public static void main(String[] args) throws Exception {
        WarehouseConfig config = new WarehouseConfig();
        File rawDir = resolve(args.length > 0 ? args[0] : config.get("local.raw.dir"));
        File outputDir = resolve(args.length > 1 ? args[1] : "local-output/offline-warehouse");

        LocalCleanLayerBuilder cleanBuilder = new LocalCleanLayerBuilder();
        Map<String, LocalTable> cleanTables = cleanBuilder.build(rawDir);

        LocalOdsLayerBuilder odsBuilder = new LocalOdsLayerBuilder();
        Map<String, LocalTable> odsTables = odsBuilder.build(cleanTables);

        LocalDwdLayerBuilder dwdBuilder = new LocalDwdLayerBuilder();
        Map<String, LocalTable> dwdTables = dwdBuilder.build(odsTables);

        LocalDwsLayerBuilder dwsBuilder = new LocalDwsLayerBuilder();
        Map<String, LocalTable> dwsTables = dwsBuilder.build(dwdTables);

        LocalAdsLayerBuilder adsBuilder = new LocalAdsLayerBuilder();
        Map<String, LocalTable> adsTables = adsBuilder.build(dwdTables, dwsTables);

        new LocalArtifactWriter().writeAll(rawDir, outputDir, cleanTables, odsTables, dwdTables, dwsTables, adsTables);

        System.out.println("Local offline warehouse build finished.");
        System.out.println("Raw input: " + rawDir.getAbsolutePath());
        System.out.println("Output dir: " + outputDir.getAbsolutePath());
        System.out.println("ADS SQL: " + new File(outputDir, "mysql/ads_seed.sql").getAbsolutePath());
    }

    private static File resolve(String path) {
        File file = new File(path);
        return file.isAbsolute() ? file : file.getAbsoluteFile();
    }
}
