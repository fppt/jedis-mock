package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.io.IOException;
import java.util.List;

@RedisCommand(value = "subscribe", transactional = false)
public class Subscribe extends AbstractRedisOperation {
    private OperationExecutorState state;

    public Subscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    protected Slice response() throws IOException {
        params().forEach(channel -> base().addSubscriber(channel, state.owner()));
        List<Slice> numSubscriptions = base().getSubscriptions(state.owner());

        return Response.subscribedToChannel(numSubscriptions);
    }
}
