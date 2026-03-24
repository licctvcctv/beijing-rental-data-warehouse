package com.beijing.wenyu.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class WarehouseConstants {

    public static final String NULL_VALUE = "\\N";
    public static final String FIELD_DELIMITER = "\t";
    public static final List<String> CATEGORIES = Collections.unmodifiableList(
            Arrays.asList("scenic", "show", "ktv", "movie", "sport")
    );

    private WarehouseConstants() {
    }
}
