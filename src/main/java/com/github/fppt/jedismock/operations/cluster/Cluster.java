package com.github.fppt.jedismock.operations.cluster;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand(value = "cluster", transactional = false)
public class Cluster implements RedisOperation {

    private static final String NODE_ID = "jedismock";

    private final OperationExecutorState state;
    private final List<Slice> params;

    Cluster(OperationExecutorState state, List<Slice> params) {
        this.state = state;
        this.params = params;
    }

    @Override
    public Slice execute() {
        if (params.isEmpty()) {
            return Response.error("ERR Wrong number of arguments for 'cluster' command");
        }
        if (!state.owner().options().isClusterModeEnabled()) {
            return Response.error("ERR This instance has cluster support disabled");
        }
        final String subcommand = params.get(0).toString();
        if ("slots".equalsIgnoreCase(subcommand)) {
            return Response.array(
                    Response.array(
                            Response.integer(0),
                            Response.integer(16383),
                            Response.array(
                                    Response.bulkString(Slice.create(state.getServerHost())),
                                    Response.integer(state.getPort()),
                                    Response.bulkString(Slice.create(NODE_ID)),
                                    Response.EMPTY_ARRAY
                            )
                    )
            );
        } else if ("nodes".equalsIgnoreCase(subcommand)) {
            return Response.bulkString(
                    Slice.create(String.format("%s %s:%d@%d myself,master - 0 1691313236000 1 connected 0-16383",
                            NODE_ID,
                            state.getServerHost(), state.getPort(),
                            state.getPort() + 10000))
            );
        } else if ("myid".equalsIgnoreCase(subcommand)) {
            return Response.bulkString(Slice.create(NODE_ID));
        } else throw new IllegalArgumentException(String.format("cluster %s subcommand not supported",
                params.get(0)));
    }
}
