package com.beijing.wenyu.etl.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CategorySchema {

    private static final Map<String, List<String>> SOURCE_FIELDS = new LinkedHashMap<String, List<String>>();

    static {
        SOURCE_FIELDS.put("scenic", Arrays.asList(
                "name", "level", "region", "address", "price", "open_time", "visit_duration",
                "best_visit_time", "source_url", "source_site", "crawl_time"
        ));
        SOURCE_FIELDS.put("show", Arrays.asList(
                "name", "show_time", "venue", "region", "price_range", "status", "attention",
                "source_url", "source_site", "crawl_time"
        ));
        SOURCE_FIELDS.put("ktv", Arrays.asList(
                "name", "region", "address", "avg_cost", "service_score", "env_score", "overall_score",
                "popularity", "business_hours", "source_url", "source_site", "crawl_time"
        ));
        SOURCE_FIELDS.put("movie", Arrays.asList(
                "name", "score", "category", "country_region", "director", "actors", "intro",
                "source_url", "source_site", "crawl_time"
        ));
        SOURCE_FIELDS.put("sport", Arrays.asList(
                "name", "venue_type", "region", "address", "score", "comment_count", "avg_cost",
                "open_time", "source_url", "source_site", "crawl_time"
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
