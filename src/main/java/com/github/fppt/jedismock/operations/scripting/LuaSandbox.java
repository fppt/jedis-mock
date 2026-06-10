package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reproduces real Redis's Lua sandbox so a script cannot reach the host or
 * tamper with the shared environment:
 * <ul>
 *   <li>the script runs in a read-only environment that delegates reads to the
 *       real globals but raises on writes and on access to an undeclared
 *       global ("Script attempted to access nonexistent global variable ...");</li>
 *   <li>the {@code redis} API table is made read-only (as {@code cjson} already
 *       is), so {@code redis.call = ...} fails;</li>
 *   <li>{@code setmetatable}/{@code getmetatable} are guarded so the metatable
 *       of a protected table cannot be replaced and basic types expose no
 *       mutable metatable;</li>
 *   <li>dangerous globals (print/dofile/loadfile and the deprecated
 *       setfenv/getfenv/newproxy) are removed, and {@code os} is reduced to
 *       {@code os.clock} backed by the injected server clock.</li>
 * </ul>
 */
final class LuaSandbox {

    private static final String READONLY_MSG = "Attempt to modify a readonly table";
    //Marks a protected table's metatable so setmetatable() can refuse to replace it.
    private static final LuaValue READONLY_MARKER = LuaValue.valueOf("__redis_readonly__");
    private static final LuaValue GLOBAL_TABLE = LuaValue.valueOf("_G");
    private static final List<String> DANGEROUS_GLOBALS =
            Arrays.asList("print", "dofile", "loadfile", "setfenv", "getfenv", "newproxy");

    private LuaSandbox() {
    }

    /**
     * Locks down {@code globals} and returns the read-only environment table the
     * user script must be loaded with.
     */
    static LuaTable install(Globals globals, OperationExecutorState state) {
        //os reduced to a single, deterministic clock backed by the server clock.
        Map<LuaValue, LuaValue> osEntries = new HashMap<>();
        osEntries.put(LuaValue.valueOf("clock"), osClock(state));
        globals.set("os", new ImmutableLuaTable(osEntries));

        //Removed globals read back as nil, so the global-access guard reports them
        //as "nonexistent" exactly like real Redis.
        for (String name : DANGEROUS_GLOBALS) {
            globals.set(name, LuaValue.NIL);
        }

        globals.set("redis", asImmutable(globals.get("redis")));

        //Real Redis exposes loadstring but only for text chunks; loading a binary
        //dump yields nil (so calling the result raises "attempt to call a nil
        //value"). luaj has no loadstring and its load() happily undumps bytecode,
        //so provide a text-only loadstring on top of load.
        globals.set("loadstring", textOnlyLoadstring(globals.get("load")));

        final LuaValue originalSetmetatable = globals.get("setmetatable");
        globals.set("getmetatable", guardedGetmetatable());

        final LuaTable sandbox = new LuaTable();
        Map<LuaValue, LuaValue> mt = new HashMap<>();
        mt.put(LuaValue.INDEX, indexGuard(globals, sandbox));
        mt.put(LuaValue.NEWINDEX, readonlyGuard());
        mt.put(READONLY_MARKER, LuaValue.TRUE);
        sandbox.setmetatable(new ImmutableLuaTable(mt));

        globals.set("setmetatable", guardedSetmetatable(originalSetmetatable));
        return sandbox;
    }

    private static LuaValue textOnlyLoadstring(LuaValue load) {
        return new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs args) {
                LuaValue chunk = args.arg1();
                //A precompiled (binary) chunk starts with the ESC signature byte;
                //refuse it as real Redis does, returning nil so the caller's call
                //of the result fails with "attempt to call a nil value".
                if (chunk.isstring()) {
                    LuaString s = chunk.checkstring();
                    if (s.m_length > 0 && s.luaByte(0) == 0x1B) {
                        return LuaValue.NIL;
                    }
                }
                return load.invoke(args);
            }
        };
    }

    private static LuaValue osClock(OperationExecutorState state) {
        return new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return LuaValue.valueOf(state.getClock().millis() / 1000.0);
            }
        };
    }

    private static LuaTable asImmutable(LuaValue table) {
        Map<LuaValue, LuaValue> entries = new HashMap<>();
        LuaValue key = LuaValue.NIL;
        while (true) {
            Varargs next = table.next(key);
            key = next.arg1();
            if (key.isnil()) {
                break;
            }
            entries.put(key, next.arg(2));
        }
        return new ImmutableLuaTable(entries);
    }

    private static LuaValue indexGuard(Globals globals, LuaTable sandbox) {
        return new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue self, LuaValue key) {
                if (key.eq_b(GLOBAL_TABLE)) {
                    //_G inside the script must be the sandbox itself, so tricks
                    //against _G hit the read-only environment, not real globals.
                    return sandbox;
                }
                LuaValue value = globals.rawget(key);
                if (value.isnil()) {
                    //Use a LuaValue message object (not a String): when caught by
                    //pcall, luaj returns getMessageObject(), which for a String-
                    //constructed LuaError falls back to the message-with-traceback.
                    throw new LuaError(LuaValue.valueOf(
                            "Script attempted to access nonexistent global variable '"
                                    + key.tojstring() + "'"));
                }
                return value;
            }
        };
    }

    private static LuaValue readonlyGuard() {
        return new ThreeArgFunction() {
            @Override
            public LuaValue call(LuaValue table, LuaValue key, LuaValue value) {
                throw new LuaError(LuaValue.valueOf(READONLY_MSG));
            }
        };
    }

    private static LuaValue guardedGetmetatable() {
        return new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue value) {
                //Only tables expose their metatable; for basic types return nil so
                //getmetatable(<basic>).__index = ... fails with "index a nil value".
                if (!value.istable()) {
                    return LuaValue.NIL;
                }
                //getmetatable() yields a Java null (not NIL) for a table without a
                //metatable; never return that to the VM.
                LuaValue metatable = value.getmetatable();
                return metatable != null ? metatable : LuaValue.NIL;
            }
        };
    }

    private static LuaValue guardedSetmetatable(LuaValue original) {
        return new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue table, LuaValue metatable) {
                if (table.istable()) {
                    //getmetatable() yields a Java null (not NIL) when absent.
                    LuaValue existing = table.getmetatable();
                    if (existing != null && existing.rawget(READONLY_MARKER).toboolean()) {
                        throw new LuaError(LuaValue.valueOf(READONLY_MSG));
                    }
                }
                return original.call(table, metatable);
            }
        };
    }
}
