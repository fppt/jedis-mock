package com.github.fppt.jedismock.operations.scripting.cjson;

import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class Encode extends OneArgFunction {

    @Override
    public LuaValue call(LuaValue arg) {
        return LuaString.valueOf(JsonSerializer.serialize(new Converter().convert(arg)));
    }

    /**
     * Converts a {@code LuaValue} into the object graph {@link JsonSerializer} writes as JSON.
     *
     * <p>A fresh instance backs every {@link Encode#call} to ensure nesting depth resets to
     * zero between separate {@code cjson.encode()} calls, including multiple calls within the
     * same script that reuse the same {@code Encode} object.
     *
     * <p>Nesting is bounded at 1000 to match real Redis's cjson default, protecting against
     * cyclic tables and runaway recursion. However, the conversion is implemented as a recursive
     * traversal, so the achievable depth also depends on the thread's stack size, not only on
     * {@code NESTING_LIMIT}. Below roughly 512 KB of thread stack—for example, JVMs configured
     * with {@code -Xss256k} or {@code -Xss512k}—a legal 1000-deep table can throw
     * {@code StackOverflowError} before the nesting limit is reached. Because
     * {@code StackOverflowError} is an {@code Error}, it is not caught by
     * {@code MockExecutor.proceed}'s {@code catch (RuntimeException e)} or Lua's {@code pcall},
     * so the client connection is left with no reply and becomes wedged.
     */
    private static final class Converter {

        private static final int NESTING_LIMIT = 1000;

        private int depth;

        private Object convert(LuaValue value) {
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
            if (value.istable()) {
                return convertContainer(value.checktable());
            }
            return value.tojstring();
        }

        /**
         * Converts an array or an object, counting how deep the recursion currently is. Siblings
         * do not count: the depth is given back once a container is fully converted, and a
         * container that fails to convert aborts the whole conversion, so there is nothing to
         * give back on that path.
         */
        private Object convertContainer(LuaTable table) {
            if (++depth > NESTING_LIMIT) {
                throw new IllegalArgumentException("Cannot serialise, excessive nesting (" + depth + ")");
            }
            Object result = isArray(table) ? toList(table) : toMap(table);
            depth--;
            return result;
        }

        private boolean isArray(LuaTable luaTable) {
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

        private Map<Object, Object> toMap(LuaTable table) {
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

        private List<Object> toList(LuaTable table) {
            //A plain loop rather than a stream pipeline: each recursive call to convert() here
            //is on the hook for the whole nesting depth, and a stream pipeline's extra frames
            //per element meaningfully lower how deep that recursion can go before the thread's
            //stack is exhausted -- verified empirically against NESTING_LIMIT below.
            int length = table.length();
            List<Object> list = new ArrayList<>(length);
            for (int i = 1; i <= length; i++) {
                list.add(convert(table.get(i)));
            }
            return list;
        }
    }
}
