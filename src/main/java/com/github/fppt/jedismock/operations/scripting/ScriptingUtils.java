package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.scripting.cjson.LuaCjsonLib;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ScriptingUtils {

    private static final String SCRIPT_RUNTIME_ERROR = "Error running script (call to function returned nil)";
    private static final String REDIS_LUA = loadResource();
    private static final Pattern LOCATION_SEPARATOR = Pattern.compile("^(user_(?:script|function):\\d+) ");
    private static final Pattern USER_SCRIPT_LINE = Pattern.compile("(@?user_(?:script|function):\\d+)");
    private static final Pattern JAVA_EXCEPTION =
            Pattern.compile("^(?:[\\w$]+\\.)+[\\w$]*(?:Exception|Error):\\s*");
    private static final Pattern ERROR_CODE = Pattern.compile("^[A-Z][A-Z0-9_]+ ");

    private ScriptingUtils() {}

    public static LuaTable createLuaSandbox(Globals globals, OperationExecutorState state) {
        /*
        An alias for 'unpack' function: unpack() was moved to table.unpack() in Lua 5.2,
        but Redis uses Lua 5.1.
         */
        globals.set("unpack", globals.load("return table.unpack(...)").checkfunction());
        globals.set("redis", globals.load(REDIS_LUA).call().checktable());
        globals.set("_mock", CoerceJavaToLua.coerce(new LuaRedisCallback(state)));
        globals.set("cjson", globals.load(new LuaCjsonLib()));
        //Install a per-instruction hook so a concurrent SCRIPT KILL can abort
        //this script, even a tight infinite loop.
        globals.load(new InterruptibleDebugLib(state.scriptingManager()));
        //Lock down the environment (read-only globals, no host access) and run the
        //user script inside it, exactly as real Redis sandboxes Lua.
        return LuaSandbox.install(globals, state);
    }

    /**
     * Build a Redis-style error reply from a raised {@link LuaError}, for an
     * actual {@code EVAL}/{@code FCALL} invocation. Real Redis (7.x):
     * <ul>
     *   <li>{@code error({err=X})} / {@code error({})} -&gt; {@code X} verbatim
     *       (or {@code "unknown error"}), ERR-prefixed when it has no code;</li>
     *   <li>{@code error("msg")} and other runtime errors -&gt; keep luaj's
     *       {@code "user_script:<line>:"} location;</li>
     *   <li>command/callback failures -&gt; unwrap luaj's
     *       {@code "vm error: java.lang.SomeException: .."} wrapper.</li>
     * </ul>
     * In every case it appends Redis's {@code " script: <scriptName>, on @user_script:N."}
     * context: real Redis's {@code luaCallFunction} always runs the user's script/
     * function through a {@code lua_pcall} message handler that captures the
     * failing source/line via {@code debug.getinfo} and attaches it to the error,
     * regardless of whether the raised value already carried a location of its
     * own. CR/LF in the message are collapsed to spaces by {@link Response#error}.
     */
    public static String scriptErrorReply(LuaError e, String scriptName) {
        return errorMessage(e, scriptName);
    }

    /**
     * Build a Redis-style error message from a raised {@link LuaError} during a
     * FUNCTION library's top-level load script (not an invocation). Unlike
     * {@link #scriptErrorReply}, this never appends the {@code " script: X, on
     * Y."} context: real Redis's {@code luaEngineCreate} runs that script through
     * a plain {@code lua_pcall} with no message handler installed at all, so no
     * source/line ever gets attached to the error &mdash; only whatever location
     * a runtime error or {@code luaL_error}/{@code error(string)} call already
     * baked into the message text itself survives.
     */
    public static String libraryLoadErrorMessage(LuaError e) {
        return errorMessage(e, null);
    }

    private static String errorMessage(LuaError e, String scriptName) {
        String raw = stripTraceback(e.getMessage());
        //Derive the script line from luaj's location before any stripping; it is
        //present for runtime errors ("user_script:N: ..") and command/callback
        //errors ("user_script:N vm error: .."). Only error({err=..})/error({})
        //tables carry no location, in which case we fall back to line 1.
        String line = lineOf(raw);
        LuaValue obj = e.getMessageObject();
        String msg;
        if (obj != null && obj.istable()) {
            LuaValue err = obj.rawget("err");
            msg = err.isnil() ? "unknown error" : err.tojstring();
        } else {
            msg = raw;
            int vm = msg.indexOf("vm error:");
            if (vm >= 0) {
                //A Redis command/callback raised: unwrap the Java exception wrapper.
                msg = stripJavaExceptionClass(msg.substring(vm + "vm error:".length()).trim());
            } else {
                //A pure Lua runtime error (incl. error("string")): keep the location.
                msg = fixLuaWording(normalizeLocation(stripAtMarker(msg)));
            }
        }
        msg = ensureErrorCode(msg);
        return scriptName == null ? msg : appendScriptContext(msg, scriptName, line);
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

    private static String lineOf(String message) {
        Matcher m = USER_SCRIPT_LINE.matcher(message);
        //Fall back to 1 only when luaj gives no location (e.g. an error() table).
        return m.find() ? m.group(1) : "@user_script:1";
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

    private static String appendScriptContext(String msg, String scriptName, String line) {
        return msg + " script: " + scriptName + ", on " + line + ".";
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
                .replace("attempt to index nil", "attempt to index a nil value")
                //luaj reports indexing/assigning a nil value as "index expected,
                //got nil"; reference Lua (and Redis) say "attempt to index ...".
                .replace("index expected, got nil", "attempt to index a nil value");
    }

    private static String ensureErrorCode(String msg) {
        if (msg.isEmpty()) {
            return "ERR " + SCRIPT_RUNTIME_ERROR;
        }
        //Keep an existing all-caps error code (ERR, WRONGTYPE, NOSCRIPT, ...);
        //otherwise add the generic ERR prefix, as real Redis does.
        return ERROR_CODE.matcher(msg).find() ? msg : "ERR " + msg;
    }

    public static LuaTable slicesToLuaValues(List<Slice> args) {
        return embedLuaListToValue(getLuaValues(args));
    }

    public static List<LuaValue> getLuaValues(List<Slice> slices) {
        return slices.stream()
                .map(Slice::data)
                .map(LuaValue::valueOf)
                .collect(Collectors.toList());
    }

    public static LuaTable embedLuaListToValue(final List<LuaValue> luaValues) {
        return LuaValue.listOf(luaValues.toArray(new LuaValue[0]));
    }

    public static Slice resolveResult(LuaValue result) {
        if (result.isnil()) {
            return Response.NULL;
        }

        switch (result.typename()) {
            case "string":
                return Response.bulkString(Slice.create(result.checkstring().m_bytes));
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
                LuaValue dbl = result.rawget("double");
                if (!dbl.isnil()) {
                    //{double=...} is the RESP3 double convention; in RESP2 real
                    //Redis returns it as a bulk string of the number.
                    return Response.bulkString(Slice.create(Double.toString(dbl.todouble())));
                }
                return Response.array(luaTableToList(result));
            case "boolean":
                return result.toboolean() ? Response.integer(1) : Response.NULL;
        }
        return Response.error(SCRIPT_RUNTIME_ERROR);
    }

    private static ArrayList<Slice> luaTableToList(LuaValue result) {
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
        try (InputStream in = ScriptingUtils.class.getResourceAsStream("/redis.lua");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
