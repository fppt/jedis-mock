package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand(value = "subscribe", transactional = false)
public class Subscribe extends AbstractRedisOperation {
    private final OperationExecutorState state;

    public Subscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        // Every argument is acknowledged separately (even a duplicate of an already
        // subscribed channel), with the client's total channel+pattern subscription
        // count. The acknowledgements are sent while still holding the global lock
        // (this runs inside MockExecutor's synchronized block). A concurrent PUBLISH
        // needs the same lock, so it cannot deliver a message to this subscriber
        // before the acks are written -- preserving Redis's ordering guarantee.
        // See issue #768.
        for (Slice channel : params()) {
            state.subscriptionRegistry().addSubscriber(channel, state.owner());
            int subscriptionsCount = state.subscriptionRegistry().getSubscriptionsCount(state.owner());
            state.owner().sendResponse(Response.subscribedToChannel(channel, subscriptionsCount), "subscribe");
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
