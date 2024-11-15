package com.github.fppt.jedismock.operations.scripting.cjson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class Encode extends OneArgFunction {

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
        if (value.isboolean()) {
            return value.toboolean();
        }
        if (value.islong()) {
            return value.tolong();
        }
        if (value.isnumber()) {
            return value.todouble();
        }
        if (value.istable() && isArray(value.checktable())) {
            return toList(value.checktable());
        }
        if (value.istable()) {
            return toMap(value.checktable());
        }
        return value.tojstring();
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
            if (key.isnil()) { // no more keys
                break;
            }
            if (!key.isint() || !indexes.remove(key.toint())) {
                return false;
            }
        }
        return true;
    }

    private static Map<Object, Object> toMap(LuaTable table) {
        Map<Object, Object> map = new HashMap<>();
        LuaValue key = LuaValue.NIL;
        while (true) {
            Varargs next = table.next(key);
            key = next.arg1();
            if (key.isnil()) { // no more keys
                break;
            }
            if (key.isboolean() || key.istable()) {
                throw new IllegalArgumentException("Unsupported key type: " + key.typename());
            }
            LuaValue value = next.arg(2);
            map.put(convert(key), convert(value));
        }
        return map;
    }

    private static List<Object> toList(LuaTable table) {
        return IntStream.rangeClosed(1, table.length())
                .mapToObj(table::get)
                .map(Encode::convert)
                .collect(Collectors.toList());
    }
}
