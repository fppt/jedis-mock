package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;
import org.slf4j.LoggerFactory;

import java.util.List;

@RedisCommand(value = "punsubscribe", transactional = false)
public class PUnsubscribe extends AbstractRedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PUnsubscribe.class);
    private final SubscriptionRegistry registry;
    private final RedisClient client;

    public PUnsubscribe(RedisBase base, SubscriptionRegistry registry, RedisClient client, List<Slice> params) {
        super(base, params);
        this.registry = registry;
        this.client = client;
    }

    @Override
    protected Slice response() {
        List<Slice> channelsToUbsubscribeFrom;
        if(params().isEmpty()){
            LOG.debug("No channels specified therefore unsubscribing from all channels");
            channelsToUbsubscribeFrom = registry.getPSubscriptions(client);
        } else {
            channelsToUbsubscribeFrom = params();
        }

        if (channelsToUbsubscribeFrom.isEmpty()) {
            // PUNSUBSCRIBE always replies: with no arguments and no subscriptions,
            // Redis sends a single acknowledgement with a nil pattern.
            int numSubscriptions = registry.getSubscriptionsCount(client);
            Slice response = Response.punsubscribe(null, numSubscriptions);
            client.sendResponse(Response.clientResponse("punsubscribe", response), "punsubscribe");
        }

        for (Slice channel : channelsToUbsubscribeFrom) {
            LOG.debug("PUnsubscribing from channel [{}]", channel);
            // Acknowledged whether or not the client was subscribed to the pattern.
            registry.removePSubscriber(channel, client);
            int numSubscriptions = registry.getSubscriptionsCount(client);
            Slice response = Response.punsubscribe(channel, numSubscriptions);
            client.sendResponse(Response.clientResponse("punsubscribe", response), "punsubscribe");
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
