package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("zinter")
class ZInter extends AbstractZInter {

    ZInter(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    protected Slice response() {
        return Response.array(getResultArray());
    }

}
