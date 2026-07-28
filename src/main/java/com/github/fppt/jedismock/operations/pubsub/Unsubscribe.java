package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;

@RedisCommand(value = "unsubscribe", transactional = false)
public class Unsubscribe extends AbstractRedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Unsubscribe.class);
    private final OperationExecutorState state;

    public Unsubscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        List<Slice> channelsToUbsubscribeFrom;
        if(params().isEmpty()){
            LOG.debug("No channels specified therefore unsubscribing from all channels");
            channelsToUbsubscribeFrom = state.subscriptionRegistry().getSubscriptions(state.owner());
        } else {
            channelsToUbsubscribeFrom = params();
        }

        if (channelsToUbsubscribeFrom.isEmpty()) {
            // UNSUBSCRIBE always replies: with no arguments and no subscriptions,
            // Redis sends a single acknowledgement with a nil channel.
            int numSubscriptions = state.subscriptionRegistry().getSubscriptionsCount(state.owner());
            Slice response = Response.unsubscribe(null, numSubscriptions);
            state.owner().sendResponse(Response.clientResponse("unsubscribe", response), "unsubscribe");
        }

        for (Slice channel : channelsToUbsubscribeFrom) {
            LOG.debug("Unsubscribing from channel [{}]", channel);
            // Acknowledged whether or not the client was subscribed to the channel.
            state.subscriptionRegistry().removeSubscriber(channel, state.owner());
            int numSubscriptions = state.subscriptionRegistry().getSubscriptionsCount(state.owner());
            Slice response = Response.unsubscribe(channel, numSubscriptions);
            state.owner().sendResponse(Response.clientResponse("unsubscribe", response), "unsubscribe");
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
