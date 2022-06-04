package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

abstract class Add extends AbstractRedisOperation {
    private final Object lock;
    Add(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    abstract void addSliceToList(List<Slice> list, Slice slice);

    protected Slice response() {
        Slice key = params().get(0);
        final RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        final List<Slice> list = listDBObj.getStoredData();

        for (int i = 1; i < params().size(); i++) {
            addSliceToList(list, params().get(i));
        }

        base().putValue(key, listDBObj);

        //Notify all waiting operations
        lock.notifyAll();
        return Response.integer(list.size());
    }
}
