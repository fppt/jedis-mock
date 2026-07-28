package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;
import java.util.stream.Collectors;

@RedisCommand(value = "pubsub", transactional = false)
public class PubSub extends AbstractRedisOperation {
    private final OperationExecutorState state;

    PubSub(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        Slice subcommand = params().get(0);
        if ("channels".equalsIgnoreCase(subcommand.toString())) {
            String pattern =
                    Utils.createRegexFromGlob(
                            params().size() > 1 ? params().get(1).toString() : "*");
            return Response.array(state.subscriptionRegistry().getChannels().stream().filter(
                    s -> s.toString().matches(pattern)
            ).map(Response::bulkString).collect(Collectors.toList()));
        } else if ("numpat".equalsIgnoreCase(subcommand.toString())) {
            return Response.integer(state.subscriptionRegistry().getNumpat());
        } else {
            return Response.error(String.format("Unsupported operation: pubsub %s", subcommand));
        }
    }
}
