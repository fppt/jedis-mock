package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractBPop;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

abstract class BPop extends AbstractBPop {

    BPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    protected AbstractRedisOperation getSize(RedisBase base, List<Slice> params) {
        return new LLen(base(), params);
    }
}
