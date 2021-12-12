package com.github.fppt.jedismock.operations.transactions;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "unwatch", transactional = false)
public class Unwatch implements RedisOperation {
    private OperationExecutorState state;

    Unwatch(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.unwatch();
        return Response.OK;
    }
}
