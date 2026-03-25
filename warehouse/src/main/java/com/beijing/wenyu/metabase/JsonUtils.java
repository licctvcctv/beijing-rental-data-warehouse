package com.beijing.wenyu.metabase;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class JsonUtils {

    private JsonUtils() {
    }

    public static Object parse(String jsonText) {
        try {
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
            engine.put("jsonText", jsonText);
            return engine.eval("Java.asJSONCompatible(JSON.parse(jsonText))");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse JSON: " + jsonText, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(Object value) {
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    public static List<Object> asList(Object value) {
        if (value instanceof List) {
            return (List<Object>) value;
        }
        return Collections.emptyList();
    }

    public static String string(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    public static int integer(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public static String quote(String value) {
        if (value == null) {
            return "null";
        }
        String escaped = value.replace("\\", "\\\\").replace("\"", "\\\"");
        escaped = escaped.replace("\r", "\\r").replace("\n", "\\n");
        return "\"" + escaped + "\"";
    }
}
