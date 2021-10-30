package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "discard", transactional = false)
public class RO_discard implements RedisOperation {
    private OperationExecutorState state;

    RO_discard(OperationExecutorState state){
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.transactionMode(false);
        state.tx().clear();
        return Response.OK;
    }
}
