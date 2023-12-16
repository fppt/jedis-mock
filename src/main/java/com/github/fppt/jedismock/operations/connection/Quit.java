package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "quit", transactional = false)
public class Quit implements RedisOperation {
    private final OperationExecutorState state;

    public Quit(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        state.owner().sendResponse(Response.clientResponse("quit", Response.OK), "quit");
        state.owner().close();
        return Response.SKIP;
    }
}
