package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("lpushx")
class LPushX extends LPush {
    LPushX(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    protected Slice response(){
        Slice key = params().get(0);
        boolean exists = base().exists(key);

        if(exists){
            return super.response();
        }

        return Response.integer(0);
    }
}
