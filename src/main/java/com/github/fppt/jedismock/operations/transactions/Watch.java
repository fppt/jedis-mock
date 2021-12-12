package com.github.fppt.jedismock.operations.transactions;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;

import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand(value = "watch", transactional = false)
public class Watch implements RedisOperation {
    private OperationExecutorState state;
    private List<Slice> keys;

    Watch(OperationExecutorState state, List<Slice> keys) {
        this.state = state;
        this.keys = keys;
    }

    @Override
    public Slice execute() {
        state.watch(keys);
        return Response.OK;
    }
}
