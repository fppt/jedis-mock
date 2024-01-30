package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "flushall")
class FlushAll implements RedisOperation {
    private final OperationExecutorState state;

    FlushAll(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.clearAll();
        return Response.OK;
    }
}
