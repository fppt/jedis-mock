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
    private final OperationExecutorState state;

    public EvalSha(final RedisBase base, final List<Slice> params, OperationExecutorState state) {
        super(base, params);
        this.state = state;
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        final String sha = params().get(0).toString();
        final String script = base().getCachedLuaScript(sha);
        if (script == null) {
            return Response.error("NOSCRIPT No matching script. Please use EVAL.");
        }
        params().set(0, Slice.create(script));
        return new Eval(base(), params(), state).response();
    }
}
