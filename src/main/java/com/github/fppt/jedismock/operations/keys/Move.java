package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("move")
public class Move implements RedisOperation {
    private final OperationExecutorState state;
    private final List<Slice> params;

    public Move(OperationExecutorState state, List<Slice> params) {
        this.params = params;
        this.state = state;
    }

    RedisBase base() {
        return state.base();
    }

    @Override
    public Slice execute() {
        if (params.size() != 2) {
            return Response.error("ERR wrong number of arguments for 'move' command");
        }
        Slice key = params.get(0);
        int destinationIdx = Utils.convertToInteger(params.get(1).toString());
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
        destinationBase.putValue(key, value);
        Long deadline = state.base().getDeadline(key);
        if (deadline != null) {
            destinationBase.setDeadline(key, deadline);
        }
        state.base().deleteValue(key);
        return Response.integer(1);
    }
}
