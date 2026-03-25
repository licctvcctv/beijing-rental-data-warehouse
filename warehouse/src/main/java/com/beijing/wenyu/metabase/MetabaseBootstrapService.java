package com.beijing.wenyu.metabase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetabaseBootstrapService {

    private final MetabaseConfig config;
    private final MetabaseHttpClient client;

    public MetabaseBootstrapService(MetabaseConfig config) {
        this.config = config;
        this.client = new MetabaseHttpClient(config);
    }

    public void bootstrap() throws InterruptedException {
        waitForMetabase();
        String sessionId = ensureAdmin();
        int databaseId = ensureDatabase(sessionId);
        List<ResolvedCard> cards = ensureCards(sessionId, databaseId);
        int dashboardId = ensureDashboard(sessionId);
        syncDashboardCards(sessionId, dashboardId, cards);
        writeBootstrapMarker(dashboardId);
        System.out.println("Metabase bootstrap finished.");
        System.out.println("Dashboard URL: " + config.getMetabasePublicUrl() + "/dashboard/" + dashboardId);
        System.out.println("Admin login: " + config.getAdminEmail() + " / " + config.getAdminPassword());
    }

    private void waitForMetabase() throws InterruptedException {
        long deadline = System.currentTimeMillis() + config.getWaitTimeoutSeconds() * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                Map<String, Object> health = JsonUtils.asMap(client.request("GET", "/api/health", null, null));
                if ("ok".equals(JsonUtils.string(health.get("status")))) {
                    return;
                }
            } catch (Exception e) {
                System.out.println("Waiting for Metabase: " + e.getMessage());
            }
            Thread.sleep(3000L);
        }
        throw new IllegalStateException("Metabase did not become ready in time.");
    }

    private String ensureAdmin() {
        Map<String, Object> properties = JsonUtils.asMap(client.request("GET", "/api/session/properties", null, null));
        String setupToken = JsonUtils.string(properties.get("setup-token"));
        if (setupToken != null && !setupToken.isEmpty()) {
            System.out.println("Metabase is not initialized. Creating admin user...");
            try {
                Object response = client.request("POST", "/api/setup", setupPayload(setupToken), null);
                String sessionId = responseSessionId(response);
                if (sessionId != null && !sessionId.isEmpty()) {
                    return sessionId;
                }
            } catch (RuntimeException error) {
                if (!String.valueOf(error.getMessage()).contains("HTTP 403")) {
                    throw error;
                }
            }
        }
        System.out.println("Logging into Metabase...");
        Object loginResponse = client.request("POST", "/api/session", loginPayload(), null);
        String sessionId = JsonUtils.string(JsonUtils.asMap(loginResponse).get("id"));
        if (sessionId == null || sessionId.isEmpty()) {
            throw new IllegalStateException("Metabase login did not return a session id.");
        }
        return sessionId;
    }

    private int ensureDatabase(String sessionId) {
        Object response = client.request("GET", "/api/database", null, sessionId);
        List<Object> databases = JsonUtils.asList(response);
        if (databases.isEmpty()) {
            databases = JsonUtils.asList(JsonUtils.asMap(response).get("data"));
        }
        for (Object entry : databases) {
            Map<String, Object> database = JsonUtils.asMap(entry);
            Map<String, Object> details = JsonUtils.asMap(database.get("details"));
            if (config.getDbName().equals(JsonUtils.string(database.get("name")))) {
                return JsonUtils.integer(database.get("id"), -1);
            }
            if (details != null) {
                String db = JsonUtils.string(details.get("db"));
                String dbName = JsonUtils.string(details.get("dbname"));
                if (config.getMysqlDatabase().equals(db) || config.getMysqlDatabase().equals(dbName)) {
                    return JsonUtils.integer(database.get("id"), -1);
                }
            }
        }
        System.out.println("Creating Metabase MySQL connection...");
        Object created = client.request("POST", "/api/database", databasePayload(), sessionId);
        return JsonUtils.integer(JsonUtils.asMap(created).get("id"), -1);
    }

    private List<ResolvedCard> ensureCards(String sessionId, int databaseId) {
        List<Object> cards = JsonUtils.asList(client.request("GET", "/api/card", null, sessionId));
        List<ResolvedCard> resolved = new ArrayList<ResolvedCard>();
        for (CardSpec spec : MetabaseCardRegistry.cards()) {
            int cardId = findCardId(cards, spec.getName());
            if (cardId > 0) {
                client.request("PUT", "/api/card/" + cardId, cardPayload(spec, databaseId), sessionId);
            } else {
                System.out.println("Creating card: " + spec.getName());
                Object created = client.request("POST", "/api/card", cardPayload(spec, databaseId), sessionId);
                cardId = JsonUtils.integer(JsonUtils.asMap(created).get("id"), -1);
            }
            resolved.add(new ResolvedCard(spec, cardId));
        }
        return resolved;
    }

    private int ensureDashboard(String sessionId) {
        List<Object> dashboards = JsonUtils.asList(client.request("GET", "/api/dashboard", null, sessionId));
        for (Object entry : dashboards) {
            Map<String, Object> dashboard = JsonUtils.asMap(entry);
            if (config.getDashboardName().equals(JsonUtils.string(dashboard.get("name")))) {
                if (!"full".equals(JsonUtils.string(dashboard.get("width")))) {
                    client.request("PUT", "/api/dashboard/" + JsonUtils.integer(dashboard.get("id"), -1),
                            "{\"width\":\"full\"}", sessionId);
                }
                return JsonUtils.integer(dashboard.get("id"), -1);
            }
        }
        System.out.println("Creating dashboard...");
        Object created = client.request("POST", "/api/dashboard", dashboardPayload(), sessionId);
        return JsonUtils.integer(JsonUtils.asMap(created).get("id"), -1);
    }

    private void syncDashboardCards(String sessionId, int dashboardId, List<ResolvedCard> cards) {
        Map<String, Object> dashboard = JsonUtils.asMap(client.request("GET", "/api/dashboard/" + dashboardId, null, sessionId));
        List<Object> dashcards = JsonUtils.asList(dashboard.get("dashcards"));
        Map<Integer, Integer> existing = new LinkedHashMap<Integer, Integer>();
        for (Object dashcardObject : dashcards) {
            Map<String, Object> dashcard = JsonUtils.asMap(dashcardObject);
            Integer cardId = JsonUtils.integer(dashcard.get("card_id"), -1);
            Integer dashcardId = JsonUtils.integer(dashcard.get("id"), -1);
            if (cardId > 0 && dashcardId > 0) {
                existing.put(cardId, dashcardId);
            }
        }
        client.request("PUT", "/api/dashboard/" + dashboardId + "/cards", dashboardCardsPayload(cards, existing), sessionId);
    }

    private int findCardId(List<Object> cards, String cardName) {
        for (Object entry : cards) {
            Map<String, Object> card = JsonUtils.asMap(entry);
            if (cardName.equals(JsonUtils.string(card.get("name")))) {
                return JsonUtils.integer(card.get("id"), -1);
            }
        }
        return -1;
    }

    private String responseSessionId(Object response) {
        Map<String, Object> map = JsonUtils.asMap(response);
        String id = JsonUtils.string(map.get("id"));
        if (id != null && !id.isEmpty()) {
            return id;
        }
        return JsonUtils.string(map.get("session_id"));
    }

    private String setupPayload(String setupToken) {
        return "{"
                + "\"token\":" + JsonUtils.quote(setupToken) + ","
                + "\"user\":{"
                + "\"first_name\":" + JsonUtils.quote(config.getAdminFirstName()) + ","
                + "\"last_name\":" + JsonUtils.quote(config.getAdminLastName()) + ","
                + "\"email\":" + JsonUtils.quote(config.getAdminEmail()) + ","
                + "\"password\":" + JsonUtils.quote(config.getAdminPassword()) + ","
                + "\"site_name\":\"Wenyu BI\""
                + "},"
                + "\"prefs\":{\"site_name\":\"Wenyu BI\",\"allow_tracking\":false},"
                + "\"database\":null"
                + "}";
    }

    private String loginPayload() {
        return "{"
                + "\"username\":" + JsonUtils.quote(config.getAdminEmail()) + ","
                + "\"password\":" + JsonUtils.quote(config.getAdminPassword())
                + "}";
    }

    private String databasePayload() {
        return "{"
                + "\"engine\":\"mysql\","
                + "\"name\":" + JsonUtils.quote(config.getDbName()) + ","
                + "\"details\":{"
                + "\"host\":" + JsonUtils.quote(config.getMysqlHost()) + ","
                + "\"port\":" + config.getMysqlPort() + ","
                + "\"db\":" + JsonUtils.quote(config.getMysqlDatabase()) + ","
                + "\"dbname\":" + JsonUtils.quote(config.getMysqlDatabase()) + ","
                + "\"user\":" + JsonUtils.quote(config.getMysqlUser()) + ","
                + "\"password\":" + JsonUtils.quote(config.getMysqlPassword()) + ","
                + "\"ssl\":false,"
                + "\"tunnel-enabled\":false"
                + "},"
                + "\"is_full_sync\":true,"
                + "\"is_on_demand\":false,"
                + "\"auto_run_queries\":true"
                + "}";
    }

    private String cardPayload(CardSpec spec, int databaseId) {
        return "{"
                + "\"name\":" + JsonUtils.quote(spec.getName()) + ","
                + "\"description\":" + JsonUtils.quote(spec.getDescription()) + ","
                + "\"display\":" + JsonUtils.quote(spec.getDisplay()) + ","
                + "\"visualization_settings\":{},"
                + "\"dataset_query\":{"
                + "\"type\":\"native\","
                + "\"database\":" + databaseId + ","
                + "\"native\":{"
                + "\"query\":" + JsonUtils.quote(spec.getQuery()) + ","
                + "\"template-tags\":{}"
                + "}"
                + "}"
                + "}";
    }

    private String dashboardPayload() {
        return "{"
                + "\"name\":" + JsonUtils.quote(config.getDashboardName()) + ","
                + "\"description\":\"自动初始化的北京娱乐方式离线数仓 BI 面板\","
                + "\"parameters\":[],"
                + "\"width\":\"full\""
                + "}";
    }

    private String dashboardCardsPayload(List<ResolvedCard> cards, Map<Integer, Integer> existing) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"cards\":[");
        for (int i = 0; i < cards.size(); i++) {
            ResolvedCard resolvedCard = cards.get(i);
            if (i > 0) {
                builder.append(",");
            }
            int dashcardId = existing.containsKey(resolvedCard.cardId) ? existing.get(resolvedCard.cardId) : -(i + 1);
            builder.append("{")
                    .append("\"id\":").append(dashcardId).append(",")
                    .append("\"card_id\":").append(resolvedCard.cardId).append(",")
                    .append("\"row\":").append(resolvedCard.spec.getRow()).append(",")
                    .append("\"col\":").append(resolvedCard.spec.getCol()).append(",")
                    .append("\"size_x\":").append(resolvedCard.spec.getSizeX()).append(",")
                    .append("\"size_y\":").append(resolvedCard.spec.getSizeY()).append(",")
                    .append("\"parameter_mappings\":[],")
                    .append("\"visualization_settings\":{},")
                    .append("\"series\":[]")
                    .append("}");
        }
        builder.append("]}");
        return builder.toString();
    }

    private void writeBootstrapMarker(int dashboardId) {
        String markerPath = config.getBootstrapMarkerPath();
        if (markerPath == null || markerPath.trim().isEmpty()) {
            return;
        }
        File markerFile = new File(markerPath);
        File parent = markerFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Unable to create marker directory: " + parent.getAbsolutePath());
        }
        String content = "metabase_status=SUCCESS\n"
                + "dashboard_url=" + config.getMetabasePublicUrl() + "/dashboard/" + dashboardId + "\n"
                + "admin_email=" + config.getAdminEmail() + "\n";
        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(markerFile, false);
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write Metabase marker: " + markerPath, e);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static class ResolvedCard {
        private final CardSpec spec;
        private final int cardId;

        private ResolvedCard(CardSpec spec, int cardId) {
            this.spec = spec;
            this.cardId = cardId;
        }
    }
}
