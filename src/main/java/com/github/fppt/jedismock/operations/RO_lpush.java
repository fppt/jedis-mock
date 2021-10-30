package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("lpush")
class RO_lpush extends RO_add {
    RO_lpush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    void addSliceToList(List<Slice> list, Slice slice) {
        list.add(0, slice);
    }
}
