package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("ping")
class Ping extends AbstractRedisOperation {
    private final OperationExecutorState state;

    Ping(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    protected Slice response() {
        Slice message = params().isEmpty() ? null : params().get(0);

        // A client in subscribe mode gets the RESP2 array reply
        // [pong, message-or-empty] instead of the usual +PONG / echo.
        if (state.subscriptionRegistry().getSubscriptionsCount(state.owner()) > 0) {
            return Response.pongInSubscribeMode(message);
        }

        if (message == null) {
            return Response.bulkString(Slice.create("PONG"));
        }

        return Response.bulkString(message);
    }
}
