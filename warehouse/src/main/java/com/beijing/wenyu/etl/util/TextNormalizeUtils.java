package com.beijing.wenyu.etl.util;

import com.beijing.wenyu.common.WarehouseConstants;

public final class TextNormalizeUtils {

    private TextNormalizeUtils() {
    }

    public static String normalize(String raw) {
        if (raw == null) {
            return WarehouseConstants.NULL_VALUE;
        }
        String value = raw.replace('\u00A0', ' ')
                .replace("\r", " ")
                .replace("\n", " ")
                .replaceAll("<[^>]*>", " ")
                .replaceAll("\\s+", " ")
                .trim();
        if (value.isEmpty() || "null".equalsIgnoreCase(value) || "暂无".equals(value)) {
            return WarehouseConstants.NULL_VALUE;
        }
        return value;
    }

    public static String nullIfBlank(String raw) {
        String normalized = normalize(raw);
        return WarehouseConstants.NULL_VALUE.equals(normalized) ? WarehouseConstants.NULL_VALUE : normalized;
    }

    public static boolean isNullValue(String value) {
        return value == null || value.trim().isEmpty() || WarehouseConstants.NULL_VALUE.equals(value.trim());
    }
}
