package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToInteger;
import static com.github.fppt.jedismock.operations.scripting.Script.getScriptSHA;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.createLuaSandbox;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.embedLuaListToValue;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.getLuaValues;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.resolveResult;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.scriptErrorReply;

@RedisCommand("eval")
public class Eval extends AbstractRedisOperation {

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
        int keysNum = convertToInteger(params().get(1).toString());
        final List<LuaValue> args = getLuaValues(params().subList(2, params().size()));
        if (keysNum < 0) {
            return Response.error("ERR Number of keys can't be negative");
        }
        if (keysNum > args.size()) {
            return Response.error("ERR Number of keys can't be greater than number of args");
        }

        final ScriptingManager scripting = state.scriptingManager();
        int selected = state.getSelected();
        //Mark the script as running *before* building the Lua environment, which
        //is slow the first time (luaj compiles REDIS_LUA, cjson and the sandbox).
        //We already hold the data lock, so a command arriving on another
        //connection meanwhile must not observe "no script running": it would then
        //block on that lock and could never be answered. Seeing a running script
        //instead makes it wait for lua-time-limit and reply -BUSY.
        scripting.start();
        try {
            globals.set("KEYS", embedLuaListToValue(args.subList(0, keysNum)));
            globals.set("ARGV", embedLuaListToValue(args.subList(keysNum, args.size())));
            final LuaTable sandbox = createLuaSandbox(globals, state);
            //Load under a fixed chunk name so error locations read "user_script:N"
            //(as in real Redis) instead of echoing the whole script body.
            final LuaValue chunk = globals.load(script, "@user_script");
            //Run the chunk inside the read-only sandbox by redirecting its _ENV
            //upvalue. We can't simply pass the sandbox as the load environment:
            //luaj only wires the per-instruction hook (used by SCRIPT KILL and
            //lua-time-limit) when the chunk's environment is a Globals instance,
            //so a plain sandbox table would make the script uninterruptible.
            if (chunk instanceof LuaClosure) {
                LuaClosure closure = (LuaClosure) chunk;
                if (closure.upValues.length > 0) {
                    closure.upValues[0].setValue(sandbox);
                }
            }
            final LuaValue result = chunk.call();
            return resolveResult(result);
        } catch (LuaError e) {
            return Response.error(scriptErrorReply(e, sha));
        } finally {
            scripting.stop();
            state.changeActiveRedisBase(selected);
        }
    }
}
