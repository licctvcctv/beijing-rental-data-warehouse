package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.common.WarehouseConstants;
import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.TextNormalizeUtils;

import java.util.Map;

public abstract class BaseCategoryCleanJob extends AbstractCleanJob {

    protected CleanRecord baseRecord(Map<String, String> rawRecord) {
        CleanRecord record = new CleanRecord(category());
        for (Map.Entry<String, String> entry : rawRecord.entrySet()) {
            record.put(entry.getKey(), TextNormalizeUtils.normalize(entry.getValue()));
        }
        return record;
    }

    protected String valueOrNull(String value) {
        return TextNormalizeUtils.isNullValue(value) ? WarehouseConstants.NULL_VALUE : value;
    }
}
