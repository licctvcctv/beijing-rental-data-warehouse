package com.beijing.wenyu.etl.cleaner;

import com.beijing.wenyu.common.HadoopConfigurationFactory;
import com.beijing.wenyu.common.WarehouseConfig;
import com.beijing.wenyu.common.WarehouseConstants;
import com.beijing.wenyu.etl.config.CategorySchema;
import com.beijing.wenyu.etl.model.CleanRecord;
import com.beijing.wenyu.etl.util.CsvLineParser;
import com.beijing.wenyu.etl.util.TextNormalizeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractCleanJob {

    protected abstract String category();

    protected abstract CleanRecord cleanRecord(Map<String, String> rawRecord);

    protected abstract List<String> outputFields();

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: <inputPath> <outputPath>");
        }
        WarehouseConfig warehouseConfig = new WarehouseConfig();
        Configuration conf = HadoopConfigurationFactory.createConfiguration(warehouseConfig);
        conf.set("warehouse.category", category());
        conf.set("warehouse.job.class", getClass().getName());
        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GenericCleanMapper.class);
        job.setReducerClass(com.beijing.wenyu.etl.util.DedupReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class GenericCleanMapper extends Mapper<Object, Text, Text, NullWritable> {

        private AbstractCleanJob delegate;
        private String category;
        private List<String> schema;

        @Override
        protected void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            this.category = configuration.get("warehouse.category");
            this.schema = CategorySchema.sourceFields(category);
            String className = configuration.get("warehouse.job.class");
            try {
                this.delegate = (AbstractCleanJob) Class.forName(className).newInstance();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to initialize clean job: " + className, e);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.trim().isEmpty() || line.startsWith("name,")) {
                return;
            }
            List<String> parsed = CsvLineParser.parse(line);
            if (parsed.size() < schema.size()) {
                return;
            }
            Map<String, String> rawRecord = new LinkedHashMap<String, String>();
            for (int i = 0; i < schema.size(); i++) {
                rawRecord.put(schema.get(i), TextNormalizeUtils.normalize(parsed.get(i)));
            }
            CleanRecord cleanRecord = delegate.cleanRecord(rawRecord);
            if (cleanRecord == null) {
                return;
            }
            StringBuilder builder = new StringBuilder();
            for (String field : delegate.outputFields()) {
                if (builder.length() > 0) {
                    builder.append(WarehouseConstants.FIELD_DELIMITER);
                }
                String fieldValue = cleanRecord.get(field);
                builder.append(fieldValue == null ? WarehouseConstants.NULL_VALUE : fieldValue);
            }
            context.write(new Text(builder.toString()), NullWritable.get());
        }
    }
}
