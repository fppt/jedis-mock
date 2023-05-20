package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.operations.scripting.Script.getScriptSHA;

@RedisCommand("eval")
public class Eval extends AbstractRedisOperation {

    private static final String SCRIPT_PARAM_ERROR = "Wrong number of arguments for EVAL";
    private static final String SCRIPT_RUNTIME_ERROR = "Error running script (call to function returned nil)";
    private final Globals globals = JsePlatform.standardGlobals();

    private final OperationExecutorState state;

    public Eval(final RedisBase base, final List<Slice> params, final OperationExecutorState state)
            throws NoSuchAlgorithmException {
        super(base, params);
        this.state = state;
    }

    @Override
    public Slice response() {
        if (params().size() < 2) {
            return Response.error(SCRIPT_PARAM_ERROR);
        }
        final String script = "local redis = {\n" +
                "  call = function(...)\n" +
                "    return _mock:call({...})\n" +
                "  end,\n" +
                "  \n" +
                "  pcall = function(...)\n" +
                "    return _mock:pcall({...})\n" +
                "  end,\n" +
                "  sha1hex = function(x)\n" +
                "    return _mock:sha1hex(x)\n" +
                "  end,\n" +
                "}\n" +
                params().get(0).toString();

        this.base().addCachedLuaScript(getScriptSHA(params().get(0).toString()), script);

        int keysNum = Integer.parseInt(params().get(1).toString());
        final List<LuaValue> args = getLuaValues(params().subList(2, params().size()));

        globals.set("KEYS", embedLuaListToValue(args.subList(0, keysNum)));
        globals.set("ARGV", embedLuaListToValue(args.subList(keysNum, args.size())));
        globals.set("_mock", CoerceJavaToLua.coerce(new LuaRedisCallback(state)));

        try {
            final LuaValue result = globals.load(script).call();
            return resolveResult(result);
        } catch (LuaError e) {
            return Response.error(String.format("Error running script: %s", e.getMessage()));
        }
    }

    private static List<LuaValue> getLuaValues(List<Slice> slices) {
        return slices.stream()
                .map(Slice::toString)
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
                return Response.bulkString(Slice.create(result.tojstring()));
            case "number":
                return Response.integer(result.tolong());
            case "table":
                if (!result.get("err").isnil()) {
                    return Response.error(result.get("err").tojstring());
                }
                if (!result.get("ok").isnil()) {
                    return resolveResult(result.get("ok"));
                }
                return Response.array(luaTableToList(result));
            case "boolean":
                return result.toboolean() ? Response.integer(1) : Response.NULL;
        }
        return Response.error(SCRIPT_RUNTIME_ERROR);
    }

    private ArrayList<Slice> luaTableToList(LuaValue result) {
        final ArrayList<Slice> list = new ArrayList<>();
        for (int i = 0; i < result.length(); i++) {
            list.add(resolveResult(result.get(i + 1)));
        }
        return list;
    }


}
