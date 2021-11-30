package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("blpop")
class BLPop extends BPop {
    BLPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    AbstractRedisOperation popper(List<Slice> params) {
        return new LPop(base(), params);
    }
}
