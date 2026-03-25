package com.beijing.wenyu.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

public class WarehouseConfig {

    private final Properties properties = new Properties();

    public WarehouseConfig() {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("warehouse.properties")) {
            if (inputStream == null) {
                throw new IllegalStateException("warehouse.properties not found in classpath");
            }
            properties.load(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load warehouse.properties", e);
        }
    }

    public String get(String key) {
        String value = resolve(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing config key: " + key);
        }
        return value.trim();
    }

    public String getOrDefault(String key, String defaultValue) {
        String value = resolve(key);
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    private String resolve(String key) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.trim().isEmpty()) {
            return systemValue;
        }
        String envKey = "WAREHOUSE_" + key.toUpperCase(Locale.ROOT).replace('.', '_');
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.trim().isEmpty()) {
            return envValue;
        }
        return properties.getProperty(key);
    }
}
