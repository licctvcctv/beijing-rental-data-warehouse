package com.beijing.wenyu.hdfs;

import com.beijing.wenyu.common.WarehouseConfig;
import com.beijing.wenyu.common.WarehouseConstants;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class RawDataUploader {

    private final WarehouseConfig config;
    private final HdfsClient hdfsClient;

    public RawDataUploader(WarehouseConfig config, HdfsClient hdfsClient) {
        this.config = config;
        this.hdfsClient = hdfsClient;
    }

    public void uploadAll(boolean overwrite) throws IOException {
        String localRawDir = config.get("local.raw.dir");
        String rawBase = config.get("hdfs.raw.base");
        Map<String, String> missingFiles = new LinkedHashMap<String, String>();

        for (String category : WarehouseConstants.CATEGORIES) {
            String fileName = config.get("local.raw." + category);
            File localFile = new File(localRawDir, fileName);
            if (!localFile.exists()) {
                missingFiles.put(category, localFile.getAbsolutePath());
                continue;
            }
            String categoryDir = rawBase + "/" + category;
            String hdfsTarget = categoryDir + "/" + fileName;
            hdfsClient.mkdir(categoryDir);
            hdfsClient.upload(localFile.getAbsolutePath(), hdfsTarget, overwrite);
            System.out.println("Uploaded " + localFile.getName() + " -> " + hdfsTarget);
        }

        if (!missingFiles.isEmpty()) {
            StringBuilder builder = new StringBuilder("Missing local raw file(s): ");
            for (Map.Entry<String, String> entry : missingFiles.entrySet()) {
                builder.append("[").append(entry.getKey()).append("=").append(entry.getValue()).append("] ");
            }
            throw new IllegalStateException(builder.toString().trim());
        }
    }
}
