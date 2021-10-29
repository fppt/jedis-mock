package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@MetaOperation("flushall")
class RO_flushall implements RedisOperation {
    private OperationExecutorState state;

    RO_flushall(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.clearAll();
        return Response.OK;
    }
}
