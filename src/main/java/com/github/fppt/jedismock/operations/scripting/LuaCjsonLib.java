package com.github.fppt.jedismock.operations.scripting;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;

public class LuaCjsonLib extends TwoArgFunction {

    @Override
    public LuaValue call(LuaValue modname, LuaValue env) {
        LuaTable cjson = new LuaTable();
        cjson.set("encode", new encode());
        cjson.set("decode", new decode());
        env.set("cjson", cjson);
        env.get("package").get("loaded").set("cjson", cjson);
        return cjson;
    }

    static class encode extends OneArgFunction {
        private final Gson gson = new GsonBuilder()
                .serializeNulls()
                .create();

        @Override
        public LuaValue call(LuaValue arg) {
            return LuaString.valueOf(gson.toJson(convert(arg)));
        }

        private static Object convert(LuaValue value) {
            if (value.isnil()) {
                return null;
            }
            if (value.islong()) {
                return value.tolong();
            }
            if (value.isnumber()) {
                return value.todouble();
            }
            if (value.isboolean()) {
                return value.toboolean();
            }
            if (value.istable() && isArray(value.checktable())) {
                return toList(value.checktable());
            }
            if (value.istable()) {
                return toMap(value.checktable());
            }
            return value.tojstring();
        }

        private static Map<Object, Object> toMap(LuaTable table) {
            Map<Object, Object> map = new HashMap<>();
            LuaValue key = LuaValue.NIL;
            while (true) {
                Varargs next = table.next(key);
                key = next.arg1();
                if (key.isnil()) {
                    break;
                }
                LuaValue value = next.arg(2);
                map.put(convert(key), convert(value));
            }
            return map;
        }

        private static List<Object> toList(LuaTable table) {
            List<Object> result = new ArrayList<>();
            for (int i = 1; i < table.length() + 1; i++) {
                result.add(convert(table.get(i)));
            }
            return result;
        }

        private static boolean isArray(LuaTable luaTable) {
            if (luaTable.length() == 0) {
                return false;
            }
            LuaValue key = LuaValue.NIL;
            Set<Integer> indexes = IntStream.rangeClosed(1, luaTable.length())
                    .boxed()
                    .collect(Collectors.toSet());
            while (true) {
                Varargs next = luaTable.next(key);
                key = next.arg1();
                if (key.isnil()) {
                    break;
                }
                if (!key.isint() || !indexes.remove(key.toint())) {
                    return false;
                }
            }
            return true;
        }
    }

    static class decode extends OneArgFunction {
        private final Gson gson = new Gson();

        @Override
        public LuaValue call(LuaValue arg) {
            Object javaObject = toJavaObject(arg.checkstring().tojstring());
            return coerceToLuaValue(javaObject);
        }

        private static LuaValue coerceToLuaValue(Object object) {
            if (object instanceof List<?>) {
                List<?> list = (List<?>) object;
                LuaValue table = LuaValue.tableOf(list.size(), list.size());
                for (int i = 0; i < list.size(); i++) {
                    table.set(i + 1, coerceToLuaValue(list.get(i)));
                }
                return table;
            }
            if (object instanceof Map<?, ?>) {
                Map<?, ?> map = (Map<?, ?>) object;
                LuaValue table = LuaValue.tableOf(map.size(), map.size());
                map.forEach((key, value) -> table.set(coerceToLuaValue(key), coerceToLuaValue(value)));
                return table;
            }
            return CoerceJavaToLua.coerce(object);
        }

        private static boolean isBoolean(String value) {
            return "true".equals(value) || "false".equals(value);
        }

        private static boolean isInteger(String value) {
            try {
                Integer.parseInt(value);
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
            return "null".equals(value);
        }

        private Object toJavaObject(String value) {
            if (value == null) {
                return null;
            }
            if (isBoolean(value)) {
                return Boolean.parseBoolean(value);
            }
            if (isInteger(value)) {
                return Integer.parseInt(value);
            }
            if (isDouble(value)) {
                return Double.parseDouble(value);
            }
            if (isNull(value)) {
                return null;
            }
            return gson.fromJson(value, Object.class);
        }
    }
}
