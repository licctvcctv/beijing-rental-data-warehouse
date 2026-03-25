package com.beijing.wenyu.metabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class MetabaseHttpClient {

    private final MetabaseConfig config;

    public MetabaseHttpClient(MetabaseConfig config) {
        this.config = config;
    }

    public Object request(String method, String path, String payload, String sessionId) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(path.startsWith("http") ? path : config.getMetabaseUrl() + path);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setConnectTimeout(30000);
            connection.setReadTimeout(30000);
            connection.setRequestProperty("Content-Type", "application/json");
            if (sessionId != null && !sessionId.isEmpty()) {
                connection.setRequestProperty("X-Metabase-Session", sessionId);
            }
            if (payload != null) {
                connection.setDoOutput(true);
                OutputStream outputStream = connection.getOutputStream();
                try {
                    outputStream.write(payload.getBytes(StandardCharsets.UTF_8));
                } finally {
                    outputStream.close();
                }
            }

            int statusCode = connection.getResponseCode();
            InputStream stream = statusCode >= 400 ? connection.getErrorStream() : connection.getInputStream();
            String body;
            try {
                body = readBody(stream);
            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
            if (statusCode >= 400) {
                throw new IllegalStateException(method + " " + path + " failed: HTTP " + statusCode + " " + body);
            }
            if (body == null || body.trim().isEmpty()) {
                return null;
            }
            return JsonUtils.parse(body);
        } catch (IOException e) {
            throw new IllegalStateException(method + " " + path + " failed: " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String readBody(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            return "";
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        try {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        } finally {
            reader.close();
        }
    }
}
