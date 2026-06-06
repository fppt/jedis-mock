package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

@RedisCommand(value = "info", transactional = false)
class Info implements RedisOperation {
    private final OperationExecutorState state;

    Info(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        //role:master line is needed for Lettuce client.
        //blocked_clients is polled by test suites (wait_for_blocked_client) to
        //synchronize before unblocking a blocking command.
        String info = "Redis Mock Server Info\r\n"
                + "role:master\r\n"
                + "# Clients\r\n"
                + "blocked_clients:" + state.blockingManager().blockedClients() + "\r\n";
        return Response.bulkString(Slice.create(info));
    }
}
