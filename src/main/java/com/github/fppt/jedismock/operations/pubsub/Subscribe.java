package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;

import java.util.List;

@RedisCommand(value = "subscribe", transactional = false)
public class Subscribe extends AbstractRedisOperation {
    private final SubscriptionRegistry registry;
    private final RedisClient client;

    public Subscribe(RedisBase base, SubscriptionRegistry registry, RedisClient client, List<Slice> params) {
        super(base, params);
        this.registry = registry;
        this.client = client;
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
        int subscriptionsCount = registry.getSubscriptionsCount(client);
        for (Slice channel : params()) {
            if (registry.addSubscriber(channel, client)) {
                subscriptionsCount++;
            }
            client.sendResponse(Response.subscribedToChannel(channel, subscriptionsCount), "subscribe");
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
