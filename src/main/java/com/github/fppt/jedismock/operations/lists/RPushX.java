package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand(value = "rpushx", arity = -3, readOnly = false)
public class RPushX extends RPush {
    RPushX(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        boolean exists = base().exists(key);

        if (exists) {
            return super.response();
        }

        return Response.integer(0);
    }
}
