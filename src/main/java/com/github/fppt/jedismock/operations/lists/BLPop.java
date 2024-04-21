package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("blpop")
class BLPop extends BPop {

    BLPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    public Slice popper(List<Slice> params) {
        Slice result = new LPop(base(), params).execute();
        return Response.array(Response.bulkString(params.get(0)), result);
    }
}
