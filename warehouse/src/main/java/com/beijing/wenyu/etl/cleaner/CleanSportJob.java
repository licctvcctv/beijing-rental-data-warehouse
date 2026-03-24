package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.CleanRuleUtils;
import com.beijing.wenyu.etl.util.NumberParseUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CleanSportJob extends BaseCategoryCleanJob {

    private static final List<String> OUTPUT_FIELDS = Arrays.asList(
            "name", "venue_type", "region", "address", "score", "comment_count", "avg_cost",
            "open_time", "source_url", "source_site", "crawl_time"
    );

    @Override
    protected String category() {
        return "sport";
    }

    @Override
    protected CleanRecord cleanRecord(Map<String, String> rawRecord) {
        CleanRecord record = baseRecord(rawRecord);
        if (!CleanRuleUtils.hasPrimaryName(record)) {
            return null;
        }
        record.put("venue_type", CleanRuleUtils.normalizeSportType(rawRecord.get("venue_type")));
        record.put("score", NumberParseUtils.extractDecimal(rawRecord.get("score")));
        record.put("comment_count", NumberParseUtils.extractInteger(rawRecord.get("comment_count")));
        record.put("avg_cost", NumberParseUtils.extractDecimal(rawRecord.get("avg_cost")));
        return record;
    }

    @Override
    protected List<String> outputFields() {
        return OUTPUT_FIELDS;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new CleanSportJob().run(args));
    }
}
