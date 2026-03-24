package com.beijing.wenyu.runner;

import com.beijing.wenyu.common.WarehouseConfig;
import com.beijing.wenyu.hdfs.HdfsClient;
import com.beijing.wenyu.hdfs.RawDataUploader;

public class UploadRunner {

    public static void main(String[] args) throws Exception {
        boolean overwrite = args.length == 0 || Boolean.parseBoolean(args[0]);
        WarehouseConfig config = new WarehouseConfig();
        try (HdfsClient hdfsClient = new HdfsClient(config)) {
            RawDataUploader uploader = new RawDataUploader(config, hdfsClient);
            uploader.uploadAll(overwrite);
            System.out.println("Raw data upload finished.");
        }
    }
}
