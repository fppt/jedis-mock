package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand(value = "client", transactional = false)
public class Client implements RedisOperation {

    private final OperationExecutorState state;
    private final List<Slice> params;

    public Client(OperationExecutorState state, List<Slice> params) {
        this.state = state;
        this.params = params;
    }

    @Override
    public Slice execute() {
        if (params.isEmpty()) {
            return Response.error("wrong number of arguments for 'client' command");
        }
        final String subcommand = params.get(0).toString();
        if ("setname".equalsIgnoreCase(subcommand)) {
            state.setClientName(params.get(1).toString());
        } else if ("getname".equalsIgnoreCase(subcommand)) {
            String name = state.getClientName();
            return name == null ? Response.NULL : Response.bulkString(Slice.create(name));
        }
        return Response.clientResponse("client", Response.OK);
    }
}
