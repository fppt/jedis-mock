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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.operations.scripting.Script.getScriptSHA;

@RedisCommand("eval")
public class Eval extends AbstractRedisOperation {

    private static final String SCRIPT_RUNTIME_ERROR = "Error running script (call to function returned nil)";
    private static final String REDIS_LUA = loadResource();
    private static final Pattern LOCATION_SEPARATOR = Pattern.compile("^(user_script:\\d+) ");
    private static final Pattern JAVA_EXCEPTION =
            Pattern.compile("^(?:[\\w$]+\\.)+[\\w$]*(?:Exception|Error):\\s*");
    private static final Pattern ERROR_CODE = Pattern.compile("^[A-Z][A-Z0-9_]+ ");
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
        final String sha = getScriptSHA(script);

        this.base().addCachedLuaScript(sha, script);

        int keysNum = Integer.parseInt(params().get(1).toString());
        final List<LuaValue> args = getLuaValues(params().subList(2, params().size()));
        if (keysNum < 0) {
            return Response.error("ERR Number of keys can't be negative");
        }
        if (keysNum > args.size()) {
            return Response.error("ERR Number of keys can't be greater than number of args");
        }

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
            //Load under a fixed chunk name so error locations read "user_script:N"
            //(as in real Redis) instead of echoing the whole script body.
            final LuaValue result = globals.load(script, "@user_script").call();
            return resolveResult(result);
        } catch (LuaError e) {
            if (scripting.isKillRequested()) {
                return Response.error("Script killed by user with SCRIPT KILL...");
            }
            return Response.error(scriptErrorReply(e, sha));
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

    /**
     * Build a Redis-style error reply from a raised {@link LuaError}. Real Redis
     * (7.x):
     * <ul>
     *   <li>{@code error({err=X})} / {@code error({})} -&gt; {@code X} verbatim
     *       (or {@code "unknown error"}), ERR-prefixed when it has no code;</li>
     *   <li>{@code error("msg")} and other runtime errors -&gt; keep luaj's
     *       {@code "user_script:<line>:"} location;</li>
     *   <li>command/callback failures -&gt; unwrap luaj's
     *       {@code "vm error: java.lang.SomeException: .."} wrapper.</li>
     * </ul>
     * In every case it appends Redis's {@code " script: <sha>, on @user_script:N."}
     * context. CR/LF in the message are collapsed to spaces by {@link Response#error}.
     */
    private static String scriptErrorReply(LuaError e, String sha) {
        LuaValue obj = e.getMessageObject();
        if (obj != null && obj.istable()) {
            LuaValue err = obj.rawget("err");
            String msg = err.isnil() ? "unknown error" : err.tojstring();
            return appendScriptContext(ensureErrorCode(msg), sha);
        }
        String msg = stripTraceback(e.getMessage());
        int vm = msg.indexOf("vm error:");
        if (vm >= 0) {
            //A Redis command/callback raised: unwrap the Java exception wrapper.
            msg = stripJavaExceptionClass(msg.substring(vm + "vm error:".length()).trim());
        } else {
            //A pure Lua runtime error (incl. error("string")): keep the location.
            msg = fixLuaWording(normalizeLocation(stripAtMarker(msg)));
        }
        return appendScriptContext(ensureErrorCode(msg), sha);
    }

    private static String stripAtMarker(String msg) {
        //luaj keeps the chunk-name "@" that Lua/Redis strip from error locations.
        return msg.startsWith("@") ? msg.substring(1) : msg;
    }

    private static String normalizeLocation(String msg) {
        //luaj separates the location from the message with a space; Lua/Redis use
        //a colon ("user_script:1: msg", not "user_script:1 msg").
        return LOCATION_SEPARATOR.matcher(msg).replaceFirst("$1: ");
    }

    private static String appendScriptContext(String msg, String sha) {
        return msg + " script: " + sha + ", on @user_script:1.";
    }

    private static String stripJavaExceptionClass(String msg) {
        //"java.lang.IllegalStateException: Wrong number..." -> "Wrong number..."
        Matcher m = JAVA_EXCEPTION.matcher(msg);
        return m.find() ? msg.substring(m.end()) : msg;
    }

    private static String fixLuaWording(String msg) {
        //luaj abbreviates the reference-Lua phrasing; restore it so error
        //patterns match across Lua implementations.
        return msg
                .replace("attempt to call nil", "attempt to call a nil value")
                .replace("attempt to index nil", "attempt to index a nil value");
    }

    private static String ensureErrorCode(String msg) {
        if (msg.isEmpty()) {
            return "ERR " + SCRIPT_RUNTIME_ERROR;
        }
        //Keep an existing all-caps error code (ERR, WRONGTYPE, NOSCRIPT, ...);
        //otherwise add the generic ERR prefix, as real Redis does.
        return ERROR_CODE.matcher(msg).find() ? msg : "ERR " + msg;
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
                LuaValue ok = result.rawget("ok");
                if (!ok.isnil()) {
                    //{ok=...} is a status (simple-string) reply, e.g. the table
                    //produced by redis.status_reply("X") -> "+X".
                    return Response.simpleString(ok.tojstring());
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
