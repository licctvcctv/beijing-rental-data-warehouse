package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.CleanRuleUtils;
import com.beijing.wenyu.etl.util.PriceParseUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CleanScenicJob extends BaseCategoryCleanJob {

    private static final List<String> OUTPUT_FIELDS = Arrays.asList(
            "name", "level", "region", "address", "price", "price_min", "price_max",
            "open_time", "visit_duration", "best_visit_time", "source_url", "source_site", "crawl_time"
    );

    @Override
    protected String category() {
        return "scenic";
    }

    @Override
    protected CleanRecord cleanRecord(Map<String, String> rawRecord) {
        CleanRecord record = baseRecord(rawRecord);
        if (!CleanRuleUtils.hasPrimaryName(record)) {
            return null;
        }
        String rawPrice = rawRecord.get("price");
        record.put("price", PriceParseUtils.normalizeScenicPriceText(rawPrice));
        record.put("price_min", PriceParseUtils.extractScenicMinPrice(rawPrice));
        record.put("price_max", PriceParseUtils.extractScenicMaxPrice(rawPrice));
        record.put("best_visit_time", CleanRuleUtils.normalizeScenicBestVisit(rawRecord.get("best_visit_time")));
        return record;
    }

    @Override
    protected List<String> outputFields() {
        return OUTPUT_FIELDS;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new CleanScenicJob().run(args));
    }
}
