package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

public class RO_auth implements RedisOperation {
    private OperationExecutorState state;

    public RO_auth(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.owner().sendResponse(Response.clientResponse("auth", Response.OK), "auth");
        return Response.SKIP;
    }
}
