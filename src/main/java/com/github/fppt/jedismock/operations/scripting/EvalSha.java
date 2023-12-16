package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("evalsha")
public class EvalSha extends AbstractRedisOperation {
    private static final String SCRIPT_PARAM_ERROR = "Wrong number of arguments for EVALSHA";

    private final OperationExecutorState state;

    public EvalSha(final RedisBase base, final List<Slice> params, OperationExecutorState state) {
        super(base, params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        if (params().size() < 2) {
            return Response.error(SCRIPT_PARAM_ERROR);
        }
        final String sha = params().get(0).toString();
        final String script = base().getCachedLuaScript(sha);
        if (script == null) {
            return Response.error("NOSCRIPT No matching script. Please use EVAL.");
        }
        params().set(0, Slice.create(script));
        return new Eval(base(), params(), state).response();
    }
}
