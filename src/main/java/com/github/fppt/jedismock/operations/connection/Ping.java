package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;

import java.util.List;

@RedisCommand("ping")
class Ping extends AbstractRedisOperation {
    private final SubscriptionRegistry registry;
    private final RedisClient client;

    Ping(RedisBase base, SubscriptionRegistry registry, RedisClient client, List<Slice> params) {
        super(base, params);
        this.registry = registry;
        this.client = client;
    }

    protected Slice response() {
        Slice message = params().isEmpty() ? null : params().get(0);

        // A client in subscribe mode gets the RESP2 array reply
        // [pong, message-or-empty] instead of the usual +PONG / echo.
        if (registry.getSubscriptionsCount(client) > 0) {
            return Response.pongInSubscribeMode(message);
        }

        if (message == null) {
            return Response.bulkString(Slice.create("PONG"));
        }

        return Response.bulkString(message);
    }
}
