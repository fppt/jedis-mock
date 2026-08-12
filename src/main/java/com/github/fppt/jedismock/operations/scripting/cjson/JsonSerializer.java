package com.github.fppt.jedismock.operations.scripting.cjson;

import java.util.List;
import java.util.Map;

/**
 * Writes the object graph {@link Encode} builds out of a {@code LuaValue} as JSON.
 *
 * <p>The graph only ever contains {@code Map}, {@code List}, {@code String}, {@code Long},
 * {@code Double}, {@code Boolean} and {@code null}, because that is all {@code Encode#convert}
 * produces; every other Lua type reaches it as a string, so anything unrecognised is written as
 * a JSON string. Nulls are always written rather than dropped, and map keys are rendered with
 * {@code String.valueOf}, so a numeric Lua key becomes a quoted numeric name.
 *
 * <p>Escaping deliberately matches what jedis-mock emitted while this was backed by gson,
 * including gson's html-safe default of escaping {@code < > & = '}. That is not what real
 * Redis does, but changing it is a behaviour change and out of scope here.
 */
final class JsonSerializer {

    private static final char LINE_SEPARATOR = 0x2028;
    private static final char PARAGRAPH_SEPARATOR = 0x2029;
    private static final String[] ESCAPES = new String[128];

    static {
        for (int i = 0; i <= 0x1f; i++) {
            ESCAPES[i] = unicodeEscape((char) i);
        }
        ESCAPES['"'] = "\\\"";
        ESCAPES['\\'] = "\\\\";
        ESCAPES['\b'] = "\\b";
        ESCAPES['\f'] = "\\f";
        ESCAPES['\n'] = "\\n";
        ESCAPES['\r'] = "\\r";
        ESCAPES['\t'] = "\\t";
        //Characters gson escaped by default so that output is safe to embed in HTML.
        ESCAPES['<'] = unicodeEscape('<');
        ESCAPES['>'] = unicodeEscape('>');
        ESCAPES['&'] = unicodeEscape('&');
        ESCAPES['='] = unicodeEscape('=');
        ESCAPES['\''] = unicodeEscape('\'');
    }

    private JsonSerializer() {
    }

    static String serialize(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out);
        return out.toString();
    }

    private static void write(Object value, StringBuilder out) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof Boolean || value instanceof Long) {
            out.append(value);
        } else if (value instanceof Double) {
            writeDouble((Double) value, out);
        } else if (value instanceof Map) {
            writeObject((Map<?, ?>) value, out);
        } else if (value instanceof List) {
            writeArray((List<?>) value, out);
        } else {
            writeString(value.toString(), out);
        }
    }

    private static void writeDouble(double value, StringBuilder out) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("Numeric values must be finite, but was " + value);
        }
        out.append(Double.toString(value));
    }

    private static void writeObject(Map<?, ?> map, StringBuilder out) {
        out.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            writeString(String.valueOf(entry.getKey()), out);
            out.append(':');
            write(entry.getValue(), out);
        }
        out.append('}');
    }

    private static void writeArray(List<?> list, StringBuilder out) {
        out.append('[');
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            write(list.get(i), out);
        }
        out.append(']');
    }

    private static void writeString(String value, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            String escape = c < ESCAPES.length ? ESCAPES[c] : unicodeSeparatorEscape(c);
            if (escape == null) {
                out.append(c);
            } else {
                out.append(escape);
            }
        }
        out.append('"');
    }

    /**
     * U+2028 and U+2029 are legal in JSON but terminate a line in JavaScript, so they are
     * escaped even though they are not control characters.
     */
    private static String unicodeSeparatorEscape(char c) {
        if (c == LINE_SEPARATOR || c == PARAGRAPH_SEPARATOR) {
            return unicodeEscape(c);
        }
        return null;
    }

    private static String unicodeEscape(char c) {
        return String.format("\\u%04x", (int) c);
    }
}
