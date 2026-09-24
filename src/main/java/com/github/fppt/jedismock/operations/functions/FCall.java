package com.github.fppt.jedismock.operations.functions;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.UpValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.List;

import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.createLuaSandbox;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.resolveResult;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.scriptErrorReply;
import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.slicesToLuaValues;

@RedisCommand("fcall")
public class FCall extends AbstractRedisOperation {
    private final OperationExecutorState state;

    public FCall(RedisBase base, List<Slice> params, OperationExecutorState state) {
        super(base, params);
        this.state = state;
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        int numKeys = parseNumKeys();
        List<Slice> keys = params().subList(2, 2 + numKeys);
        List<Slice> args = params().subList(2 + numKeys, params().size());

        String functionName = params().get(0).toString();
        FunctionInfo functionInfo = base().getLuaFunctionInfo(functionName);
        if (functionInfo == null) {
            throw new ArgumentException("ERR Function not found");
        }

        ScriptingManager scripting = state.scriptingManager();
        LuaValue result;
        try {
            //Mark the script as running *before* building the Lua environment, which
            //is slow the first time (luaj compiles REDIS_LUA, cjson and the sandbox).
            //We already hold the data lock, so a command arriving on another
            //connection meanwhile must not observe "no script running": it would then
            //block on that lock and could never be answered. Seeing a running script
            //instead makes it wait for lua-time-limit and reply -BUSY.
            scripting.startFunction(functionName, "FCALL", params());

            LuaTable luaKeys = slicesToLuaValues(keys);
            LuaTable luaArgs = slicesToLuaValues(args);

            LuaTable sandbox = createLuaSandbox(JsePlatform.standardGlobals(), state);

            //Redirect the library's *shared* _ENV upvalue (captured once at FUNCTION
            //LOAD time), not just this closure's own upvalues: a registered function
            //that only calls a private helper defined alongside it in the library may
            //not capture _ENV itself at all (it has no need to, if it never touches a
            //global directly) while that helper does, and they share the same upvalue
            //box. Patching only `function`'s own upvalues would silently miss this.
            UpValue sharedEnvironment = base().getLuaFunctionEnvironment(functionInfo);
            if (sharedEnvironment != null) {
                sharedEnvironment.setValue(sandbox);
            }

            try {
                result = functionInfo.getFunction().call(luaKeys, luaArgs);
            } catch (LuaError e) {
                return Response.error(scriptErrorReply(e, functionName));
            }
        } finally {
            scripting.stop();
        }

        return resolveResult(result);
    }

    private int parseNumKeys() {
        int numKeys;
        try {
            numKeys = Integer.parseInt(params().get(1).toString());
        } catch (NumberFormatException e) {
            throw new ArgumentException("ERR Bad number of keys provided");
        }
        if (numKeys < 0) {
            throw new ArgumentException("ERR Number of keys can't be negative");
        }
        if (numKeys > params().size() - 2) {
            throw new ArgumentException("ERR Number of keys can't be greater than number of args");
        }
        return numKeys;
    }
}
