package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;

import java.util.List;

@RedisCommand("publish")
class Publish extends AbstractRedisOperation {
    private final SubscriptionRegistry registry;

    Publish(RedisBase base, SubscriptionRegistry registry, List<Slice> params) {
        super(base, params);
        this.registry = registry;
    }

    protected Slice response(){
        Slice channel = params().get(0);
        Slice message = params().get(1);
        return Response.integer(registry.publish(channel, message));
    }
}
