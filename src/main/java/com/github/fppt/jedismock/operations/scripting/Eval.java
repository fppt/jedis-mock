package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.scripting.cjson.LuaCjsonLib;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.operations.scripting.Script.getScriptSHA;

@RedisCommand("eval")
public class Eval extends AbstractRedisOperation {

    private static final String SCRIPT_RUNTIME_ERROR = "Error running script (call to function returned nil)";
    private static final String REDIS_LUA = loadResource();
    private final Globals globals = JsePlatform.standardGlobals();
    private final OperationExecutorState state;

    public Eval(final RedisBase base, final List<Slice> params, final OperationExecutorState state) {
        super(base, params);
        this.state = state;
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    public Slice response() {
        final String script = params().get(0).toString();

        this.base().addCachedLuaScript(getScriptSHA(script), script);

        int keysNum = Integer.parseInt(params().get(1).toString());
        final List<LuaValue> args = getLuaValues(params().subList(2, params().size()));

        /*
        An alias for 'unpack' function: unpack() was moved to table.unpack() in Lua 5.2,
        but Redis uses Lua 5.1.
         */
        globals.set("unpack", globals.load("return table.unpack(...)").checkfunction());
        globals.set("redis", globals.load(REDIS_LUA).call().checktable());
        globals.set("KEYS", embedLuaListToValue(args.subList(0, keysNum)));
        globals.set("ARGV", embedLuaListToValue(args.subList(keysNum, args.size())));
        globals.set("_mock", CoerceJavaToLua.coerce(new LuaRedisCallback(state)));
        globals.set("cjson", globals.load(new LuaCjsonLib()));
        //Install a per-instruction hook so a concurrent SCRIPT KILL can abort
        //this script, even a tight infinite loop.
        final ScriptingManager scripting = state.scriptingManager();
        globals.load(new InterruptibleDebugLib(scripting));
        int selected = state.getSelected();
        scripting.start();
        try {
            final LuaValue result = globals.load(script).call();
            return resolveResult(result);
        } catch (LuaError e) {
            if (scripting.isKillRequested()) {
                return Response.error("Script killed by user with SCRIPT KILL...");
            }
            return Response.error(String.format("Error running script: %s", stripTraceback(e.getMessage())));
        } finally {
            scripting.stop();
            state.changeActiveRedisBase(selected);
        }
    }

    private static String stripTraceback(String message) {
        if (message == null) {
            return "";
        }
        //luaj appends a multi-line Lua stack traceback; drop it so the message
        //is a single line (real Redis does not echo the traceback in the reply).
        int trace = message.indexOf("stack traceback:");
        return (trace >= 0 ? message.substring(0, trace) : message).trim();
    }

    private static List<LuaValue> getLuaValues(List<Slice> slices) {
        return slices.stream()
                .map(Slice::data)
                .map(LuaValue::valueOf)
                .collect(Collectors.toList());
    }

    public static LuaTable embedLuaListToValue(final List<LuaValue> luaValues) {
        return LuaValue.listOf(luaValues.toArray(new LuaValue[0]));
    }

    private Slice resolveResult(LuaValue result) {
        if (result.isnil()) {
            return Response.NULL;
        }

        switch (result.typename()) {
            case "string":
                return Response.bulkString(Slice.create(((LuaString) result).m_bytes));
            case "number":
                return Response.integer(result.tolong());
            case "table":
                //Use raw access (no metatable) throughout, matching real Redis,
                //which converts a returned table with lua_rawget*. Otherwise a
                //table carrying an __index metamethod would trigger it here.
                if (!result.rawget("err").isnil()) {
                    return Response.error(result.rawget("err").tojstring());
                }
                if (!result.rawget("ok").isnil()) {
                    return resolveResult(result.rawget("ok"));
                }
                return Response.array(luaTableToList(result));
            case "boolean":
                return result.toboolean() ? Response.integer(1) : Response.NULL;
        }
        return Response.error(SCRIPT_RUNTIME_ERROR);
    }

    private ArrayList<Slice> luaTableToList(LuaValue result) {
        //Like Redis: raw-get indices 1, 2, ... and stop at the first nil.
        final ArrayList<Slice> list = new ArrayList<>();
        for (int i = 1; ; i++) {
            LuaValue element = result.rawget(i);
            if (element.isnil()) {
                break;
            }
            list.add(resolveResult(element));
        }
        return list;
    }


    private static String loadResource() {
        try (InputStream in = Eval.class.getResourceAsStream("/redis.lua");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
