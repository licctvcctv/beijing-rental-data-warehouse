package com.beijing.wenyu.local;

import com.beijing.wenyu.common.WarehouseConstants;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class LocalValueUtils {

    private LocalValueUtils() {
    }

    public static String decimalOrZero(String value) {
        return isNull(value) ? "0" : value;
    }

    public static String decimalOrFallback(String value, String fallback) {
        if (!isNull(value)) {
            return value;
        }
        return isNull(fallback) ? "0" : fallback;
    }

    public static double parseDouble(String value) {
        if (isNull(value)) {
            return 0D;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0D;
        }
    }

    public static long parseLong(String value) {
        if (isNull(value)) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public static boolean isNull(String value) {
        return value == null || value.trim().isEmpty() || WarehouseConstants.NULL_VALUE.equals(value.trim());
    }

    public static String formatDecimal(double value, int scale) {
        BigDecimal decimal = BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
        decimal = decimal.stripTrailingZeros();
        return decimal.scale() < 0 ? decimal.setScale(0).toPlainString() : decimal.toPlainString();
    }

    public static String safe(String value) {
        return isNull(value) ? WarehouseConstants.NULL_VALUE : value;
    }

    public static String sqlLiteral(String value) {
        if (isNull(value)) {
            return "NULL";
        }
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}
