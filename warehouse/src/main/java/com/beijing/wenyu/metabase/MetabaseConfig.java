package com.beijing.wenyu.metabase;

public class MetabaseConfig {

    private final String metabaseUrl;
    private final String metabasePublicUrl;
    private final String adminEmail;
    private final String adminPassword;
    private final String adminFirstName;
    private final String adminLastName;
    private final String dbName;
    private final String dashboardName;
    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUser;
    private final String mysqlPassword;
    private final int waitTimeoutSeconds;
    private final String bootstrapMarkerPath;

    public MetabaseConfig() {
        this.metabaseUrl = env("METABASE_URL", "http://localhost:3000");
        this.metabasePublicUrl = env("METABASE_PUBLIC_URL", this.metabaseUrl);
        this.adminEmail = env("METABASE_ADMIN_EMAIL", "admin@wenyu.local");
        this.adminPassword = env("METABASE_ADMIN_PASSWORD", "Admin@123456");
        this.adminFirstName = env("METABASE_ADMIN_FIRST_NAME", "Wenyu");
        this.adminLastName = env("METABASE_ADMIN_LAST_NAME", "Admin");
        this.dbName = env("METABASE_DB_NAME", "Wenyu MySQL");
        this.dashboardName = env("METABASE_DASHBOARD_NAME", "北京娱乐方式离线数仓 BI 看板");
        this.mysqlHost = env("METABASE_MYSQL_HOST", "mysql");
        this.mysqlPort = Integer.parseInt(env("METABASE_MYSQL_PORT", "3306"));
        this.mysqlDatabase = env("METABASE_MYSQL_DATABASE", "wenyu_result");
        this.mysqlUser = env("METABASE_MYSQL_USER", "wenyu");
        this.mysqlPassword = env("METABASE_MYSQL_PASSWORD", "wenyu123");
        this.waitTimeoutSeconds = Integer.parseInt(env("METABASE_WAIT_TIMEOUT_SECONDS", "180"));
        this.bootstrapMarkerPath = env("METABASE_BOOTSTRAP_MARKER_PATH", "");
    }

    private String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    public String getMetabaseUrl() {
        return metabaseUrl;
    }

    public String getMetabasePublicUrl() {
        return metabasePublicUrl;
    }

    public String getAdminEmail() {
        return adminEmail;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public String getAdminFirstName() {
        return adminFirstName;
    }

    public String getAdminLastName() {
        return adminLastName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getDashboardName() {
        return dashboardName;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    public String getMysqlDatabase() {
        return mysqlDatabase;
    }

    public String getMysqlUser() {
        return mysqlUser;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public int getWaitTimeoutSeconds() {
        return waitTimeoutSeconds;
    }

    public String getBootstrapMarkerPath() {
        return bootstrapMarkerPath;
    }
}
