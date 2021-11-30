package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("brpop")
class BRPop extends BPop {

    BRPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    AbstractRedisOperation popper(List<Slice> params) {
        return new RPop(base(), params);
    }
}
