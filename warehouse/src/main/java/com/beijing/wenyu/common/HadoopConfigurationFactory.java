package com.beijing.wenyu.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

public final class HadoopConfigurationFactory {

    private static final String[] RESOURCE_FILES = {
            "core-site.xml",
            "hdfs-site.xml",
            "mapred-site.xml",
            "yarn-site.xml"
    };

    private HadoopConfigurationFactory() {
    }

    public static Configuration createConfiguration(WarehouseConfig config) {
        Configuration configuration = new Configuration();
        String configDir = resolveConfigDir();
        if (configDir != null) {
            for (String resourceFile : RESOURCE_FILES) {
                addResourceIfExists(configuration, configDir, resourceFile);
            }
        }
        configuration.set("fs.defaultFS", config.get("hdfs.uri"));
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.replication", "1");
        return configuration;
    }

    private static String resolveConfigDir() {
        String systemValue = System.getProperty("HADOOP_CONF_DIR");
        if (systemValue != null && !systemValue.trim().isEmpty()) {
            return systemValue.trim();
        }
        String envValue = System.getenv("HADOOP_CONF_DIR");
        if (envValue != null && !envValue.trim().isEmpty()) {
            return envValue.trim();
        }
        return null;
    }

    private static void addResourceIfExists(Configuration configuration, String configDir, String resourceFile) {
        File file = new File(configDir, resourceFile);
        if (file.isFile()) {
            configuration.addResource(new Path(file.getAbsolutePath()));
        }
    }
}
