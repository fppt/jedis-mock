package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand(value = "move", arity = 3, readOnly = false)
public class Move extends AbstractRedisOperation {
    private final OperationExecutorState state;

    public Move(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        int destinationIdx = Utils.convertToInteger(params().get(1).toString());
        RMDataStructure value = state.base().getValue(key);
        if (value == null) {
            // Source doesn't exist
            return Response.integer(0);
        }
        RedisBase destinationBase = state.base(destinationIdx);
        if (destinationBase.exists(key)) {
            //Destination already existed
            return Response.integer(0);
        }
        destinationBase.putValue(key, value, null);
        Long deadline = state.base().getDeadline(key);
        if (deadline != null) {
            destinationBase.setDeadline(key, deadline);
        }
        state.base().deleteValue(key);
        state.base().notifyKeyspaceEvent(KeyspaceEvent.MOVE_FROM, key);
        //move_to belongs to the destination database, so it is published on its channels
        destinationBase.notifyKeyspaceEvent(KeyspaceEvent.MOVE_TO, key);
        return Response.integer(1);
    }
}
