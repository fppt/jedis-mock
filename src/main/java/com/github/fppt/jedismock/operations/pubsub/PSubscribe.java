package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.io.IOException;
import java.util.List;

@RedisCommand(value = "psubscribe", transactional = false)
public class PSubscribe extends AbstractRedisOperation {
    private final OperationExecutorState state;
    public PSubscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    protected Slice response() throws IOException {
        params().forEach(pattern -> base().subscribeByPattern(pattern, state.owner()));
        List<Slice> numSubscriptions = base().getPSubscriptions(state.owner());

        return Response.psubscribedToChannel(numSubscriptions);
    }
}
