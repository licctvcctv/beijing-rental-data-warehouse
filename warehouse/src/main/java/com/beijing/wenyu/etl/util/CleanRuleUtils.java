package com.beijing.wenyu.etl.util;

import com.beijing.wenyu.common.WarehouseConstants;
import com.beijing.wenyu.etl.model.CleanRecord;

import java.util.Locale;

public final class CleanRuleUtils {

    private CleanRuleUtils() {
    }

    public static boolean hasPrimaryName(CleanRecord record) {
        return !TextNormalizeUtils.isNullValue(record.get("name"));
    }

    public static String normalizeShowStatus(String raw) {
        String value = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(value)) {
            return WarehouseConstants.NULL_VALUE;
        }
        value = value.replace("【", "").replace("】", "");
        if (value.contains("预售")) {
            return "预售中";
        }
        if (value.contains("售票") || value.contains("开售")) {
            return "售票中";
        }
        if (value.contains("结束") || value.contains("停售")) {
            return "已结束";
        }
        return value;
    }

    public static String normalizeSportType(String raw) {
        String value = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(value)) {
            return WarehouseConstants.NULL_VALUE;
        }
        if (value.contains("游泳")) {
            return "游泳馆";
        }
        if (value.contains("滑冰") || value.contains("滑雪")) {
            return "冰雪场馆";
        }
        if (value.contains("网球")) {
            return "网球场馆";
        }
        if (value.contains("高尔夫")) {
            return "高尔夫场馆";
        }
        if (value.contains("体育中心")) {
            return "体育中心";
        }
        if (value.contains("体育馆")) {
            return "综合体育馆";
        }
        return value;
    }

    public static String normalizeScenicBestVisit(String raw) {
        String value = TextNormalizeUtils.normalize(raw);
        if (WarehouseConstants.NULL_VALUE.equals(value)) {
            return WarehouseConstants.NULL_VALUE;
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.contains("全年")) {
            return "全年";
        }
        return value.replace(" ", "");
    }
}
