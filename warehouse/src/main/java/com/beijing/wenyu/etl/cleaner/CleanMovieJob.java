package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.CleanRuleUtils;
import com.beijing.wenyu.etl.util.NumberParseUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CleanMovieJob extends BaseCategoryCleanJob {

    private static final List<String> OUTPUT_FIELDS = Arrays.asList(
            "name", "score", "category", "country_region", "director", "actors",
            "intro", "source_url", "source_site", "crawl_time"
    );

    @Override
    protected String category() {
        return "movie";
    }

    @Override
    protected CleanRecord cleanRecord(Map<String, String> rawRecord) {
        CleanRecord record = baseRecord(rawRecord);
        if (!CleanRuleUtils.hasPrimaryName(record)) {
            return null;
        }
        record.put("score", NumberParseUtils.extractDecimal(rawRecord.get("score")));
        return record;
    }

    @Override
    protected List<String> outputFields() {
        return OUTPUT_FIELDS;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new CleanMovieJob().run(args));
    }
}
