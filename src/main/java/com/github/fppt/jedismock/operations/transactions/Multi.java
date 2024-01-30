package com.github.fppt.jedismock.operations.transactions;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "multi", transactional = false)
public class Multi implements RedisOperation {
    private final OperationExecutorState state;

    Multi(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        if (state.isTransactionModeOn()) {
            return Response.error("ERR MULTI calls can not be nested");
        } else {
            state.transactionMode(true);
            return Response.OK;
        }
    }
}
