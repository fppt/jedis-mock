package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "auth", transactional = false)
public class Auth implements RedisOperation {
    private final OperationExecutorState state;

    public Auth(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.owner().sendResponse(Response.clientResponse("auth", Response.OK), "auth");
        return Response.SKIP;
    }
}
