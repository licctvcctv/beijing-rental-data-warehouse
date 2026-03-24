package com.beijing.wenyu.etl.util;

import com.beijing.wenyu.common.WarehouseConstants;

import java.util.Arrays;
import java.util.List;

public final class RegionNormalizeUtils {

    private static final List<String> REGIONS = Arrays.asList(
            "东城区", "西城区", "朝阳区", "海淀区", "丰台区", "石景山区", "门头沟区", "房山区",
            "通州区", "顺义区", "昌平区", "大兴区", "怀柔区", "平谷区", "密云区", "延庆区"
    );

    private RegionNormalizeUtils() {
    }

    public static String normalize(String region, String address) {
        String normalized = extractRegion(TextNormalizeUtils.normalize(region));
        if (!WarehouseConstants.NULL_VALUE.equals(normalized)) {
            return normalized;
        }
        return extractRegion(TextNormalizeUtils.normalize(address));
    }

    public static String extractRegion(String text) {
        if (TextNormalizeUtils.isNullValue(text)) {
            return WarehouseConstants.NULL_VALUE;
        }
        for (String region : REGIONS) {
            if (text.contains(region)) {
                return region;
            }
            String shortName = region.replace("区", "");
            if (text.contains(shortName)) {
                return region;
            }
        }
        if (text.contains("北京")) {
            return "北京市";
        }
        return WarehouseConstants.NULL_VALUE;
    }
}
