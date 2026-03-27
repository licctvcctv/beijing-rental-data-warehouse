package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.common.WarehouseConstants;
import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.NumberParseUtils;
import com.beijing.wenyu.etl.util.TextNormalizeUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CleanRentalJob extends BaseCategoryCleanJob {

    @Override
    protected String category() {
        return "rental";
    }

    @Override
    protected List<String> outputFields() {
        return Arrays.asList(
                "fy_id", "fy_title", "fy_type", "fy_status", "platform",
                "xzq", "sq", "jd", "wd", "month_zj", "jzmj", "is_dt", "zx_qk"
        );
    }

    @Override
    protected CleanRecord cleanRecord(Map<String, String> rawRecord) {
        String fyId = rawRecord.get("fy_id");
        if (TextNormalizeUtils.isNullValue(fyId)) {
            return null;
        }
        String monthZj = NumberParseUtils.extractInteger(rawRecord.get("month_zj"));
        if ("0".equals(monthZj) || WarehouseConstants.NULL_VALUE.equals(monthZj)) {
            return null;
        }
        CleanRecord record = new CleanRecord(category());
        for (Map.Entry<String, String> entry : rawRecord.entrySet()) {
            record.put(entry.getKey(), TextNormalizeUtils.normalize(entry.getValue()));
        }
        record.put("month_zj", monthZj);
        record.put("jzmj", NumberParseUtils.extractDecimal(rawRecord.get("jzmj")));
        record.put("jd", NumberParseUtils.extractDecimal(rawRecord.get("jd")));
        record.put("wd", NumberParseUtils.extractDecimal(rawRecord.get("wd")));
        return record;
    }
}
