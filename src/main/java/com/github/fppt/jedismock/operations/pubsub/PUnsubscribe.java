package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;

@RedisCommand(value = "punsubscribe", transactional = false)
public class PUnsubscribe extends AbstractRedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PUnsubscribe.class);
    private final OperationExecutorState state;

    public PUnsubscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        List<Slice> channelsToUbsubscribeFrom;
        if(params().isEmpty()){
            LOG.debug("No channels specified therefore unsubscribing from all channels");
            channelsToUbsubscribeFrom = state.subscriptionRegistry().getPSubscriptions(state.owner());
        } else {
            channelsToUbsubscribeFrom = params();
        }

        if (channelsToUbsubscribeFrom.isEmpty()) {
            // PUNSUBSCRIBE always replies: with no arguments and no subscriptions,
            // Redis sends a single acknowledgement with a nil pattern.
            int numSubscriptions = state.subscriptionRegistry().getSubscriptionsCount(state.owner());
            Slice response = Response.punsubscribe(null, numSubscriptions);
            state.owner().sendResponse(Response.clientResponse("punsubscribe", response), "punsubscribe");
        }

        for (Slice channel : channelsToUbsubscribeFrom) {
            LOG.debug("PUnsubscribing from channel [{}]", channel);
            // Acknowledged whether or not the client was subscribed to the pattern.
            state.subscriptionRegistry().removePSubscriber(channel, state.owner());
            int numSubscriptions = state.subscriptionRegistry().getSubscriptionsCount(state.owner());
            Slice response = Response.punsubscribe(channel, numSubscriptions);
            state.owner().sendResponse(Response.clientResponse("punsubscribe", response), "punsubscribe");
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
