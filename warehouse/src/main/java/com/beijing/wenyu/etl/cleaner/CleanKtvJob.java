package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.CleanRuleUtils;
import com.beijing.wenyu.etl.util.NumberParseUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CleanKtvJob extends BaseCategoryCleanJob {

    private static final List<String> OUTPUT_FIELDS = Arrays.asList(
            "name", "region", "address", "avg_cost", "service_score", "env_score", "overall_score",
            "popularity", "business_hours", "source_url", "source_site", "crawl_time"
    );

    @Override
    protected String category() {
        return "ktv";
    }

    @Override
    protected CleanRecord cleanRecord(Map<String, String> rawRecord) {
        CleanRecord record = baseRecord(rawRecord);
        if (!CleanRuleUtils.hasPrimaryName(record)) {
            return null;
        }
        record.put("avg_cost", NumberParseUtils.extractDecimal(rawRecord.get("avg_cost")));
        record.put("service_score", NumberParseUtils.extractDecimal(rawRecord.get("service_score")));
        record.put("env_score", NumberParseUtils.extractDecimal(rawRecord.get("env_score")));
        record.put("overall_score", NumberParseUtils.extractDecimal(rawRecord.get("overall_score")));
        record.put("popularity", NumberParseUtils.extractInteger(rawRecord.get("popularity")));
        return record;
    }

    @Override
    protected List<String> outputFields() {
        return OUTPUT_FIELDS;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new CleanKtvJob().run(args));
    }
}
