package com.beijing.wenyu.common;

import java.io.IOException;
import java.io.InputStream;
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
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing config key: " + key);
        }
        return value.trim();
    }

    public String getOrDefault(String key, String defaultValue) {
        String value = properties.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }
}
