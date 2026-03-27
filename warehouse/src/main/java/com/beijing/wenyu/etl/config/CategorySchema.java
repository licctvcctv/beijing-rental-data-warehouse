package com.beijing.wenyu.etl.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CategorySchema {

    private static final Map<String, List<String>> SOURCE_FIELDS = new LinkedHashMap<String, List<String>>();

    static {
        SOURCE_FIELDS.put("rental", Arrays.asList(
                "fy_id", "fy_title", "fy_type", "fy_status", "platform",
                "xzq", "sq", "jd", "wd", "month_zj", "jzmj", "is_dt", "zx_qk"
        ));
    }

    private CategorySchema() {
    }

    public static List<String> sourceFields(String category) {
        List<String> fields = SOURCE_FIELDS.get(category);
        if (fields == null) {
            throw new IllegalArgumentException("Unsupported category: " + category);
        }
        return Collections.unmodifiableList(fields);
    }
}
