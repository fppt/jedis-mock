package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
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

        // check for target type before popping
        if (base().exists(target) && !(base().getValue(target) instanceof RMList)) {
            throw new IllegalArgumentException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        //Pop last one. Its notifications are suppressed because Redis reports
        //the push into the destination *before* the pop from the source.
        RPop pop = new RPop(base(), Collections.singletonList(source));
        pop.doNotPublishEvents();
        Slice result = pop.execute();
        if(result.equals(NULL)) return NULL;
        boolean sourceEmptied = !base().exists(source);

        Slice valueToPush = SliceParser.consumeParameter(result.data());

        //Push it into the other list (which reports its own lpush)
        new LPush(state, Arrays.asList(target, valueToPush)).execute();

        base().notifyKeyspaceEvent(KeyspaceEvent.RPOP, source);
        if (sourceEmptied) {
            base().notifyKeyspaceEvent(KeyspaceEvent.DEL, source);
        }

        return result;
    }
}
