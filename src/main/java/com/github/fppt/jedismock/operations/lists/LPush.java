package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("lpush")
class LPush extends Add {
    LPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    void addSliceToList(List<Slice> list, Slice slice) {
        list.add(0, slice);
    }
}
