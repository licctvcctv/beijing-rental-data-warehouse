package com.beijing.wenyu.etl.util;

import com.beijing.wenyu.common.WarehouseConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PriceParseUtils {

    private static final Pattern PRICE_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)");
    private static final Pattern SCENIC_PRICE_SEGMENT_PATTERN = Pattern.compile("(?:门票|票价|联票|成人票|通票)[:：]?([^。；;]*)");

    private PriceParseUtils() {
    }

    public static String normalizePriceText(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return text;
        }
        List<Double> prices = extractPrices(text);
        if (prices.isEmpty()) {
            return text.contains("免费") ? "免费" : WarehouseConstants.NULL_VALUE;
        }
        if (prices.size() == 1) {
            return format(prices.get(0));
        }
        return format(prices.get(0)) + "-" + format(prices.get(prices.size() - 1));
    }

    public static String extractMinPrice(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        if (text.contains("免费")) {
            return "0";
        }
        List<Double> prices = extractPrices(text);
        return prices.isEmpty() ? WarehouseConstants.NULL_VALUE : format(prices.get(0));
    }

    public static String extractMaxPrice(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        if (text.contains("免费")) {
            return "0";
        }
        List<Double> prices = extractPrices(text);
        return prices.isEmpty() ? WarehouseConstants.NULL_VALUE : format(prices.get(prices.size() - 1));
    }

    public static String normalizeScenicPriceText(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return text;
        }
        List<Double> prices = extractScenicPrices(text);
        if (prices.isEmpty()) {
            return text.contains("免费") ? "免费" : WarehouseConstants.NULL_VALUE;
        }
        if (prices.size() == 1) {
            return format(prices.get(0));
        }
        return format(prices.get(0)) + "-" + format(prices.get(prices.size() - 1));
    }

    public static String extractScenicMinPrice(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        if (containsOnlyFreeSignals(text)) {
            return "0";
        }
        List<Double> prices = extractScenicPrices(text);
        return prices.isEmpty() ? WarehouseConstants.NULL_VALUE : format(prices.get(0));
    }

    public static String extractScenicMaxPrice(String raw) {
        String text = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        if (containsOnlyFreeSignals(text)) {
            return "0";
        }
        List<Double> prices = extractScenicPrices(text);
        return prices.isEmpty() ? WarehouseConstants.NULL_VALUE : format(prices.get(prices.size() - 1));
    }

    private static boolean containsOnlyFreeSignals(String text) {
        return text.contains("免费") && !PRICE_PATTERN.matcher(text).find();
    }

    private static List<Double> extractScenicPrices(String text) {
        List<Double> values = new ArrayList<Double>();
        Matcher segmentMatcher = SCENIC_PRICE_SEGMENT_PATTERN.matcher(text);
        while (segmentMatcher.find()) {
            values.addAll(extractPrices(segmentMatcher.group(1)));
        }
        if (!values.isEmpty()) {
            Collections.sort(values);
            return values;
        }
        if (text.contains("免费")) {
            values.add(0D);
        }
        values.addAll(extractPrices(text));
        Collections.sort(values);
        return values;
    }

    private static List<Double> extractPrices(String text) {
        Matcher matcher = PRICE_PATTERN.matcher(text.replace(",", " "));
        List<Double> values = new ArrayList<Double>();
        while (matcher.find()) {
            values.add(Double.parseDouble(matcher.group(1)));
        }
        Collections.sort(values);
        return values;
    }

    private static String format(Double value) {
        if (value == null) {
            return WarehouseConstants.NULL_VALUE;
        }
        if (Math.floor(value) == value) {
            return String.valueOf(value.longValue());
        }
        return String.valueOf(value);
    }
}
