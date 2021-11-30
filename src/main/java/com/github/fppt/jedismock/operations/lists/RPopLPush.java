package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.server.Response.NULL;

@RedisCommand("rpoplpush")
class RPopLPush extends AbstractRedisOperation {
    private final OperationExecutorState state;
    RPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    protected Slice response() {
        Slice source = params().get(0);
        Slice target = params().get(1);

        //Pop last one
        Slice result = new RPop(base(), Collections.singletonList(source)).execute();
        if(result.equals(NULL)) return NULL;

        Slice valueToPush = SliceParser.consumeParameter(result.data());

        //Push it into the other list
        new LPush(state, Arrays.asList(target, valueToPush)).execute();

        return result;
    }
}
