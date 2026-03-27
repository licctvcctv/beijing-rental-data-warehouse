package com.beijing.wenyu.runner;

import com.beijing.wenyu.common.WarehouseConfig;
import com.beijing.wenyu.common.WarehouseConstants;
import com.beijing.wenyu.etl.cleaner.AbstractCleanJob;
import com.beijing.wenyu.etl.cleaner.CleanRentalJob;

import java.util.LinkedHashMap;
import java.util.Map;

public class EtlRunner {

    public static void main(String[] args) throws Exception {
        WarehouseConfig config = new WarehouseConfig();
        String rawBase = config.get("hdfs.raw.base");
        String cleanBase = config.get("hdfs.clean.base");

        Map<String, AbstractCleanJob> jobs = new LinkedHashMap<String, AbstractCleanJob>();
        jobs.put("rental", new CleanRentalJob());

        if (args.length > 0) {
            String category = args[0];
            AbstractCleanJob job = jobs.get(category);
            if (job == null) {
                throw new IllegalArgumentException("Unsupported category: " + category);
            }
            String input = rawBase + "/" + category;
            String output = cleanBase + "/" + category;
            System.exit(job.run(new String[]{input, output}));
            return;
        }

        for (String category : WarehouseConstants.CATEGORIES) {
            AbstractCleanJob job = jobs.get(category);
            int code = job.run(new String[]{rawBase + "/" + category, cleanBase + "/" + category});
            if (code != 0) {
                throw new IllegalStateException("Clean job failed: " + category);
            }
        }
        System.out.println("All clean jobs finished.");
    }
}
