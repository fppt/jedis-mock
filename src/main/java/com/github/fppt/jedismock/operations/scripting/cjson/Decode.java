package com.github.fppt.jedismock.operations.scripting.cjson;

import com.google.gson.Gson;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;

import java.util.List;
import java.util.Map;

class Decode extends OneArgFunction {

    private static final String JSON_NULL = "null";

    private final Gson gson = new Gson();

    @Override
    public LuaValue call(LuaValue arg) {
        String toDecode = arg.checkstring().tojstring();
        if (isSurroundedByQuotes(toDecode)) {
            return LuaString.valueOf(cutQuotes(toDecode));
        }
        Object javaObject = toJavaObject(toDecode);
        if (javaObject instanceof String) {
            throw new IllegalArgumentException("Invalid JSON string");
        }
        return coerceToLuaValue(javaObject);
    }

    private static boolean isSurroundedByQuotes(String value) {
        return value.startsWith("\"") && value.endsWith("\"");
    }

    private static String cutQuotes(String toDecode) {
        return toDecode.substring(1, toDecode.length() - 1);
    }

    private Object toJavaObject(String value) {
        if (isNull(value)) {
            return null;
        }
        if (isBoolean(value)) {
            return Boolean.parseBoolean(value);
        }
        if (isLong(value)) {
            return Long.parseLong(value);
        }
        if (isDouble(value)) {
            return Double.parseDouble(value);
        }
        return gson.fromJson(value, Object.class);
    }

    private static LuaValue coerceToLuaValue(Object object) {
        if (object instanceof List<?>) {
            List<?> list = (List<?>) object;
            LuaTable table = LuaValue.tableOf(list.size(), 0);
            for (int i = 0; i < list.size(); i++) {
                table.set(i + 1, coerceToLuaValue(list.get(i)));
            }
            return table;
        }
        if (object instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) object;
            LuaTable table = LuaValue.tableOf(0, map.size());
            map.forEach((key, value) -> table.set(CoerceJavaToLua.coerce(key), coerceToLuaValue(value)));
            return table;
        }
        return CoerceJavaToLua.coerce(object);
    }

    private static boolean isBoolean(String value) {
        return Boolean.TRUE.toString().equals(value) || Boolean.FALSE.toString().equals(value);
    }

    private static boolean isLong(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isDouble(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isNull(String value) {
        return value == null || JSON_NULL.equals(value);
    }
}
