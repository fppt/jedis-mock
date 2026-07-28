package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;

import java.util.List;

@RedisCommand(value = "psubscribe", transactional = false)
public class PSubscribe extends AbstractRedisOperation {
    private final SubscriptionRegistry registry;
    private final RedisClient client;

    public PSubscribe(RedisBase base, SubscriptionRegistry registry, RedisClient client, List<Slice> params) {
        super(base, params);
        this.registry = registry;
        this.client = client;
    }

    @Override
    protected Slice response() {
        // Every argument is acknowledged separately (even a duplicate of an already
        // subscribed pattern), with the client's total channel+pattern subscription
        // count. The acknowledgements are sent while still holding the global lock
        // (this runs inside MockExecutor's synchronized block). A concurrent PUBLISH
        // needs the same lock, so it cannot deliver a pmessage to this subscriber
        // before the acks are written -- preserving Redis's ordering guarantee.
        // See issue #768.
        for (Slice pattern : params()) {
            registry.subscribeByPattern(pattern, client);
            int subscriptionsCount = registry.getSubscriptionsCount(client);
            client.sendResponse(Response.psubscribedToPattern(pattern, subscriptionsCount), "psubscribe");
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
