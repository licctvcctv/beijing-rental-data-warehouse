package com.beijing.wenyu.etl.util;

import com.beijing.wenyu.common.WarehouseConstants;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NumberParseUtils {

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)");
    private static final Pattern INTEGER_PATTERN = Pattern.compile("(\\d+)");

    private NumberParseUtils() {
    }

    public static String extractDecimal(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        Matcher matcher = DECIMAL_PATTERN.matcher(text);
        return matcher.find() ? matcher.group(1) : WarehouseConstants.NULL_VALUE;
    }

    public static String extractInteger(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        Matcher matcher = INTEGER_PATTERN.matcher(text.replace(",", ""));
        return matcher.find() ? matcher.group(1) : WarehouseConstants.NULL_VALUE;
    }
}
