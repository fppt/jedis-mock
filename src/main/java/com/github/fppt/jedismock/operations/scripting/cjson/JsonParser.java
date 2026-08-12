package com.github.fppt.jedismock.operations.scripting.cjson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A recursive-descent JSON parser producing the object graph {@link Decode} coerces into Lua
 * values: {@link Map} for objects (in document order), {@link List} for arrays, {@link String},
 * {@link Double}, {@link Boolean} and {@code null}.
 *
 * <p>Every JSON number becomes a {@code Double}, including integral ones: {@code Decode} handles
 * a top-level integer itself, and this preserves what jedis-mock did while gson parsed into
 * {@code Object} for it.
 *
 * <p>Anything that is not well-formed JSON raises {@link IllegalArgumentException}, which cjson
 * surfaces to the script as an error. That is stricter than the previous gson-backed
 * implementation, which parsed with a lenient reader and therefore also accepted unquoted names,
 * single-quoted strings, comments, {@code NaN}/{@code Infinity} and an empty document (as
 * {@code nil}). Real Redis rejects all of those too, so this only moves the mock closer to it.
 *
 * <p>Nesting is bounded, because parsing is recursive and an unbounded depth would exhaust the
 * stack: a {@code StackOverflowError} is an {@code Error}, so neither {@code MockExecutor} nor
 * Lua's {@code pcall} would catch it and the connection would be left with no reply at all. The
 * bound reproduces gson's, which rejected a 256th level of nesting.
 */
final class JsonParser {

    /** Maximum number of nested arrays and objects, matching gson's default nesting limit. */
    private static final int NESTING_LIMIT = 255;

    private final String input;
    private int pos;
    private int depth;

    private JsonParser(String input) {
        this.input = input;
    }

    static Object parse(String json) {
        JsonParser parser = new JsonParser(json);
        parser.skipWhitespace();
        Object value = parser.readValue();
        parser.skipWhitespace();
        if (parser.pos != json.length()) {
            throw parser.error("Trailing data after the JSON value");
        }
        return value;
    }

    private Object readValue() {
        char c = peek();
        switch (c) {
            case '{':
            case '[':
                return readContainer(c);
            case '"':
                return readString();
            case 't':
                expect("true");
                return Boolean.TRUE;
            case 'f':
                expect("false");
                return Boolean.FALSE;
            case 'n':
                expect("null");
                return null;
            default:
                return readNumber();
        }
    }

    /**
     * Reads an array or an object, counting how deep the recursion currently is. Siblings do not
     * count: the depth is given back once a container is complete, and a container that fails to
     * parse aborts the whole parse, so there is nothing to give back on that path.
     */
    private Object readContainer(char open) {
        if (++depth > NESTING_LIMIT) {
            throw error("Nesting limit " + NESTING_LIMIT + " reached");
        }
        Object container = open == '{' ? readObject() : readArray();
        depth--;
        return container;
    }

    private Map<String, Object> readObject() {
        Map<String, Object> object = new LinkedHashMap<>();
        pos++;
        skipWhitespace();
        if (peek() == '}') {
            pos++;
            return object;
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') {
                throw error("Expected a member name");
            }
            String name = readString();
            skipWhitespace();
            if (next() != ':') {
                throw error("Expected ':' after a member name");
            }
            skipWhitespace();
            object.put(name, readValue());
            skipWhitespace();
            char c = next();
            if (c == '}') {
                return object;
            }
            if (c != ',') {
                throw error("Expected ',' or '}'");
            }
        }
    }

    private List<Object> readArray() {
        List<Object> array = new ArrayList<>();
        pos++;
        skipWhitespace();
        if (peek() == ']') {
            pos++;
            return array;
        }
        while (true) {
            skipWhitespace();
            array.add(readValue());
            skipWhitespace();
            char c = next();
            if (c == ']') {
                return array;
            }
            if (c != ',') {
                throw error("Expected ',' or ']'");
            }
        }
    }

    private String readString() {
        pos++;
        StringBuilder value = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') {
                return value.toString();
            }
            if (c == '\\') {
                value.append(readEscape());
            } else {
                value.append(c);
            }
        }
    }

    private char readEscape() {
        char c = next();
        switch (c) {
            case '"':
            case '\\':
            case '/':
                return c;
            case 'b':
                return '\b';
            case 'f':
                return '\f';
            case 'n':
                return '\n';
            case 'r':
                return '\r';
            case 't':
                return '\t';
            case 'u':
                return readUnicodeEscape();
            default:
                throw error("Unsupported escape sequence: \\" + c);
        }
    }

    private char readUnicodeEscape() {
        int codePoint = 0;
        for (int i = 0; i < 4; i++) {
            int digit = Character.digit(next(), 16);
            if (digit < 0) {
                throw error("Malformed \\u escape sequence");
            }
            codePoint = codePoint * 16 + digit;
        }
        return (char) codePoint;
    }

    private Double readNumber() {
        int start = pos;
        if (peek() == '-') {
            pos++;
        }
        readDigits();
        if (pos < input.length() && input.charAt(pos) == '.') {
            pos++;
            readDigits();
        }
        if (pos < input.length() && isExponentMarker(input.charAt(pos))) {
            pos++;
            if (pos < input.length() && isSign(input.charAt(pos))) {
                pos++;
            }
            readDigits();
        }
        return Double.valueOf(input.substring(start, pos));
    }

    private void readDigits() {
        int start = pos;
        while (pos < input.length() && isDigit(input.charAt(pos))) {
            pos++;
        }
        if (pos == start) {
            throw error("Expected a digit");
        }
    }

    private void expect(String literal) {
        if (!input.startsWith(literal, pos)) {
            throw error("Expected '" + literal + "'");
        }
        pos += literal.length();
    }

    private void skipWhitespace() {
        while (pos < input.length() && isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }

    private char peek() {
        if (pos >= input.length()) {
            throw error("Unexpected end of the JSON value");
        }
        return input.charAt(pos);
    }

    private char next() {
        char c = peek();
        pos++;
        return c;
    }

    private IllegalArgumentException error(String message) {
        return new IllegalArgumentException(message + " at position " + pos);
    }

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isExponentMarker(char c) {
        return c == 'e' || c == 'E';
    }

    private static boolean isSign(char c) {
        return c == '+' || c == '-';
    }
}
