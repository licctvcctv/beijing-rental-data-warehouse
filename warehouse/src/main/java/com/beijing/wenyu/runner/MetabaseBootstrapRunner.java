package com.beijing.wenyu.runner;

import com.beijing.wenyu.metabase.MetabaseBootstrapService;
import com.beijing.wenyu.metabase.MetabaseConfig;

public class MetabaseBootstrapRunner {

    public static void main(String[] args) throws Exception {
        MetabaseConfig config = new MetabaseConfig();
        new MetabaseBootstrapService(config).bootstrap();
    }
}
