package com.github.fppt.jedismock.operations.transactions;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "discard", transactional = false)
public class Discard implements RedisOperation {
    private OperationExecutorState state;

    Discard(OperationExecutorState state){
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.transactionMode(false);
        state.tx().clear();
        return Response.OK;
    }
}
