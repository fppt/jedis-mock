package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("zdiffstore")
class ZDiffStore extends AbstractZDiff {

    ZDiffStore(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    protected KeyspaceEvent storeEvent() {
        return KeyspaceEvent.ZDIFFSTORE;
    }

    @Override
    protected Slice response() {
        return Response.integer(getResultSize());
    }

}
