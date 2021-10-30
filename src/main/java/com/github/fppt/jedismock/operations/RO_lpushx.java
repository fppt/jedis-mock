package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("lpushx")
class RO_lpushx extends RO_lpush {
    RO_lpushx(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    Slice response(){
        Slice key = params().get(0);
        Slice data = base().getSlice(key);

        if(data != null){
            return super.response();
        }

        return Response.integer(0);
    }
}
