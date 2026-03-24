package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.CleanRuleUtils;
import com.beijing.wenyu.etl.util.PriceParseUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CleanShowJob extends BaseCategoryCleanJob {

    private static final List<String> OUTPUT_FIELDS = Arrays.asList(
            "name", "show_time", "venue", "region", "price_range", "price_min", "price_max",
            "status", "attention", "source_url", "source_site", "crawl_time"
    );

    @Override
    protected String category() {
        return "show";
    }

    @Override
    protected CleanRecord cleanRecord(Map<String, String> rawRecord) {
        CleanRecord record = baseRecord(rawRecord);
        if (!CleanRuleUtils.hasPrimaryName(record)) {
            return null;
        }
        String rawPrice = rawRecord.get("price_range");
        record.put("price_range", PriceParseUtils.normalizePriceText(rawPrice));
        record.put("price_min", PriceParseUtils.extractMinPrice(rawPrice));
        record.put("price_max", PriceParseUtils.extractMaxPrice(rawPrice));
        record.put("status", CleanRuleUtils.normalizeShowStatus(rawRecord.get("status")));
        return record;
    }

    @Override
    protected List<String> outputFields() {
        return OUTPUT_FIELDS;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new CleanShowJob().run(args));
    }
}
