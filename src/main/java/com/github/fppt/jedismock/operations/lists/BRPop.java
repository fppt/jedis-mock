package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.List;

@RedisCommand("brpop")
class BRPop extends BPop {

    BRPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    public Slice popper(List<Slice> params) {
        Slice result = new RPop(base(), params).execute();
        return Response.array(Arrays.asList(Response.bulkString(params.get(0)), result));
    }
}
