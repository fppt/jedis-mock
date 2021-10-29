package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@MetaOperation("multi")
public class RO_multi implements RedisOperation {
    private OperationExecutorState state;

    RO_multi(OperationExecutorState state){
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.newTransaction();
        return Response.OK;
    }
}
